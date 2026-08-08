package HttkWorkflow;

use strict;
use warnings;
use POSIX qw(strftime);

our $VERSION = 2;

my $PYTHON = 'HTTK_WORKFLOW_PYTHON';
my $BRIDGE = 'httk.workflow._shell_bridge';
my $REFUSED = 2;

package HttkWorkflow::BridgeError;

use strict;
use warnings;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;

sub new {
    my ($class, $kind, $message) = @_;
    return bless { kind => $kind, message => $message }, $class;
}

sub kind { return $_[0]->{kind}; }
sub message { return $_[0]->{message}; }

package HttkWorkflow::Runner;

use strict;
use warnings;

sub new {
    my ($class, %args) = @_;
    return bless {
        workflow => $args{workflow},
        steps => $args{steps} || [],
        handlers => {},
    }, $class;
}

sub step {
    my ($self, $name, $handler) = @_;
    $self->{handlers}{$name} = $handler;
    return $self;
}

sub _valid_name {
    my ($name) = @_;
    return defined($name) && $name ne '' && $name =~ /\A[A-Za-z0-9._-]+\z/;
}

sub _validate {
    my ($self) = @_;
    my $workflow = $self->{workflow};
    my $steps = $self->{steps};
    if (!defined($workflow) || $workflow eq '' || !@$steps) {
        print STDERR "httk-workflow: a runner needs a workflow name and at least one step\n";
        return 0;
    }
    if (!_valid_name($workflow)) {
        print STDERR "httk-workflow: workflow name $workflow cannot name a runner\n";
        return 0;
    }
    my %seen;
    for my $name (@$steps) {
        if (!_valid_name($name)) {
            print STDERR "httk-workflow: step name $name cannot name a Perl step handler\n";
            return 0;
        }
        if ($seen{$name}++) {
            print STDERR "httk-workflow: step $name is already registered on the $workflow runner\n";
            return 0;
        }
        if (ref($self->{handlers}{$name}) ne 'CODE') {
            print STDERR "httk-workflow: the $workflow runner declares step $name but registers no handler\n";
            return 0;
        }
    }
    for my $name (keys %{ $self->{handlers} }) {
        if (!$seen{$name}) {
            print STDERR "httk-workflow: step $name has a handler but is not declared on the $workflow runner\n";
            return 0;
        }
    }
    return 1;
}

sub _description {
    my ($self) = @_;
    my @steps = sort @{ $self->{steps} };
    return '{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ['
        . join(', ', map { '"' . $_ . '"' } @steps)
        . '], "workflow": "' . $self->{workflow} . "\"}\n";
}

sub main {
    my ($self) = @_;
    exit $REFUSED unless $self->_validate();

    if ((grep { $_ eq '--describe' } @ARGV)
        || (defined($ENV{HTTK_WORKFLOW_DESCRIBE}) && $ENV{HTTK_WORKFLOW_DESCRIBE} eq '1')) {
        print $self->_description();
        exit 0;
    }

    $ENV{HTTK_WORKFLOW_RUNNER_WORKFLOW} = $self->{workflow};
    $ENV{HTTK_WORKFLOW_RUNNER_STEPS} = join("\n", @{ $self->{steps} });

    my $attempt = HttkWorkflow::Attempt->new();
    my $step;
    my $started = eval {
        $step = $attempt->_read('begin');
        1;
    };
    exit $REFUSED unless $started && defined($step);
    $ENV{HTTK_WORKFLOW_STEP} = $step;
    exit 0 if $attempt->_outcome_published();

    my $handler = $self->{handlers}{$step};
    if (ref($handler) ne 'CODE') {
        my $status = eval { $attempt->_command('fail-unknown-step') };
        exit 0 if !$@ && defined($status) && $status == 0;
        exit $REFUSED;
    }

    my ($returned, $error);
    my $ran = eval {
        $returned = $handler->($attempt);
        1;
    };
    $error = $@ unless $ran;
    if (!$ran || (defined($returned) && $returned != 0)) {
        my $code = $ran ? int($returned) : $REFUSED;
        my $message = $ran
            ? "$step exited with status $code"
            : (defined($error) && $error ne '' ? "$error" : "$step exited with status $code");
        $message =~ s/[\r\n]+\z//;
        eval { $attempt->_command('abort', '--exception', 'PerlError', '--message', $message); };
        exit $code;
    }

    if (!$attempt->_outcome_published()) {
        my $status = eval { $attempt->_command('fail-no-outcome') };
        exit $REFUSED if $@ || !defined($status) || $status != 0;
    }
    my $status = eval { $attempt->_command('environment-log') };
    exit $REFUSED if $@ || !defined($status) || $status != 0;
    exit 0;
}

package HttkWorkflow::Attempt;

use strict;
use warnings;

sub new { return bless {}, $_[0]; }

sub _bridge {
    my ($self, $capture, @argv) = @_;
    my $python = $ENV{HTTK_WORKFLOW_PYTHON};
    if (!defined($python) || $python eq '') {
        print STDERR "httk-workflow: HTTK_WORKFLOW_PYTHON is not set by the workflow manager\n";
        die HttkWorkflow::BridgeError->new('PythonUnset',
            'HTTK_WORKFLOW_PYTHON is not set by the workflow manager');
    }
    if ($capture) {
        my $fh;
        if (!open($fh, '-|', $python, '-m', $BRIDGE, @argv)) {
            die HttkWorkflow::BridgeError->new('Spawn', "could not start the httk-workflow bridge: $!");
        }
        local $/;
        my $output = <$fh>;
        $output = '' unless defined($output);
        close($fh);
        my $status = $?;
        $status = ($status & 127) ? $REFUSED : ($status >> 8);
        $output =~ s/\n+\z//;
        return ($status, $output);
    }
    my $raw = system($python, '-m', $BRIDGE, @argv);
    if ($raw == -1) {
        die HttkWorkflow::BridgeError->new('Spawn', "could not start the httk-workflow bridge: $!");
    }
    return ($raw & 127) ? $REFUSED : ($raw >> 8);
}

sub _command {
    my ($self, @argv) = @_;
    my ($status) = $self->_bridge(0, @argv);
    return $status;
}

sub _read {
    my ($self, @argv) = @_;
    my ($status, $output) = $self->_bridge(1, @argv);
    return $output if $status == 0;
    return undef if $status == 1;
    die HttkWorkflow::BridgeError->new('Refused', 'the httk-workflow bridge refused the call');
}

sub _args {
    my ($value) = @_;
    return () unless defined($value);
    return @$value if ref($value) eq 'ARRAY';
    return ($value);
}

sub _read_named {
    my ($self, $verb, $name, $fallback, $has_fallback) = @_;
    return $has_fallback ? $self->_read($verb, $name, '--default', $fallback) : $self->_read($verb, $name);
}

sub _outcome_published {
    my ($self) = @_;
    my $control = $ENV{HTTK_WORKFLOW_CONTROL_DIR};
    $control = '.' if !defined($control) || $control eq '';
    return -d "$control/outcome.ready";
}

sub context {
    my ($self, $field) = @_;
    return defined($field) ? $self->_read('context', $field) : $self->_read('context');
}

sub parameter {
    my ($self, $name, @rest) = @_;
    return $self->_read_named('parameter', $name, $rest[0], @rest ? 1 : 0);
}

sub setting {
    my ($self, $name, @rest) = @_;
    return $self->_read_named('setting', $name, $rest[0], @rest ? 1 : 0);
}

sub environment {
    my ($self, $name, @rest) = @_;
    return $self->_read_named('environment', $name, $rest[0], @rest ? 1 : 0);
}

sub state_get { my ($self, $name) = @_; return $self->_read('state-get', $name); }
sub state_set { my ($self, $name, $value) = @_; return $self->_command('state-set', $name, $value); }
sub state_delete { my ($self, $name) = @_; return $self->_command('state-delete', $name); }
sub state_merge { my ($self, $assignments) = @_; return $self->_command('state-merge', _args($assignments)); }

sub declaration { my ($self, $name) = @_; return $self->_read('declaration', $name); }
sub declare { my ($self, $name, $file) = @_; return $self->_command('declare', $name, $file); }

sub runlog_note { my ($self, $message) = @_; return $self->_command('runlog', 'note', $message); }
sub runlog_headline { my ($self, $message) = @_; return $self->_command('runlog', 'headline', $message); }
sub runlog_append {
    my ($self, $message, $files) = @_;
    return $self->_command('runlog', 'files', $message, _args($files));
}

# The bridge deliberately has no log subcommand; this matches the Bash/Rust
# SDKs' local timestamped stderr helper.
sub log {
    my ($self, $level, $message) = @_;
    print STDERR POSIX::strftime('%Y-%m-%dT%H:%M:%SZ', gmtime()) . " [$level] $message\n";
    return 0;
}

sub put { my ($self, $source, $destination) = @_; return $self->_read('put', $source, $destination); }
sub remove {
    my ($self, $destination, $missing_ok) = @_;
    return $missing_ok ? $self->_read('remove', $destination, '--missing-ok') : $self->_read('remove', $destination);
}
sub spawn {
    my ($self, $label, $args) = @_;
    return $self->_read('spawn', $label, _args($args));
}
sub children {
    my ($self, $selection) = @_;
    return $self->_read('children') unless defined($selection);
    $selection = "--$selection" unless $selection =~ /^--/;
    return $self->_read('children', $selection);
}
sub child { my ($self, $label, $field) = @_; return $self->_read('child', $label, $field); }

sub advance {
    my ($self, $next_step, $args) = @_;
    return $self->_command('advance', $next_step, _args($args));
}
sub gather {
    my ($self, $next_step, @rest) = @_;
    my $options = ref($rest[0]) eq 'HASH' ? $rest[0] : { @rest };
    my @args = ('gather', $next_step);
    for my $option (['when', '--when'], ['count', '--count'], ['on_impossible', '--on-impossible'], ['priority', '--priority']) {
        my ($key, $flag) = @$option;
        push @args, $flag, $options->{$key} if defined($options->{$key});
    }
    return $self->_command(@args);
}
sub succeed { my ($self) = @_; return $self->_command('succeed'); }
sub fail {
    my ($self, $code, $message, $retryable) = @_;
    return $retryable ? $self->_command('fail', $code, $message, '--retryable') : $self->_command('fail', $code, $message);
}
sub retry { my ($self, $reason) = @_; return $self->_command('retry', $reason); }
sub pause { my ($self, $reason) = @_; return $self->_command('pause', $reason); }

sub batch { my ($self) = @_; return $self->_command('batch'); }
sub job_prepare { my ($self, $destination, $spec) = @_; return $self->_read('job-prepare', $destination, $spec); }
sub workdir_apply { my ($self, $spec) = @_; return $self->_read('workdir-apply', $spec); }
sub run { my ($self, $args) = @_; return $self->_command('run', _args($args)); }
sub calc { my ($self, $expression) = @_; return $self->_read('calc', $expression); }
sub template_render {
    my ($self, $template, $output, $values) = @_;
    return $self->_command('template', $template, $output, $values);
}
sub compress { my ($self, $args) = @_; return $self->_command('compress', _args($args)); }
sub decompress { my ($self, $args) = @_; return $self->_command('decompress', _args($args)); }

sub invoke {
    my ($self, @args) = @_;
    @args = @{ $args[0] } if @args == 1 && ref($args[0]) eq 'ARRAY';
    return $self->_command(@args);
}

1;
