#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use File::Copy qw(copy);
use File::Spec;

use lib "$FindBin::Bin/../../src/httk/workflow/native/perl";
use HttkWorkflow;

my @collect = qw(INCAR KPOINTS OUTCAR CONTCAR OSZICAR vasprun.xml vasp-run-report.json);

sub env_or {
    my ($name, $fallback) = @_;
    return defined($ENV{$name}) && $ENV{$name} ne '' ? $ENV{$name} : $fallback;
}

sub stage_input {
    my ($attempt, $job_dir, $parameter, $fallback, $destination) = @_;
    my $relative = $attempt->parameter($parameter, $fallback);
    return -1 unless defined($relative);
    my $source = File::Spec->catfile($job_dir, $relative);
    return 0 unless -f $source;
    return copy($source, $destination) ? 1 : -1;
}

sub step_prepare {
    my ($attempt) = @_;
    my $job_dir = env_or('HTTK_WORKFLOW_JOB_DIR', '.');
    if (stage_input($attempt, $job_dir, 'poscar', 'files/POSCAR', 'POSCAR') <= 0) {
        $attempt->fail('vasp.input_missing', 'the starting structure is not in this payload', 0);
        return 0;
    }
    stage_input($attempt, $job_dir, 'incar', 'files/INCAR', 'INCAR');
    $attempt->runlog_note('prepared a relaxation');
    $attempt->advance('run', []);
    return 0;
}

sub step_run {
    my ($attempt) = @_;
    my $from_parameter = $attempt->parameter('vasp_command', '');
    my $command = $attempt->setting('vasp.command', $from_parameter);
    $command = '' unless defined($command);
    if ($command =~ /^\s*$/) {
        $attempt->fail(
            'vasp.command_missing',
            'no VASP command is configured: set it with '
                . "httk workflow workspace settings set vasp.command '...', or set "
                . 'HTTK_VASP_COMMAND, or give the job a vasp_command parameter',
            0,
        );
        return 0;
    }

    my $timeout = $attempt->parameter('timeout', '86400');
    my @tokens = split /\s+/, $command;
    my @args = ('--timeout', $timeout, '--report', 'vasp-run-report.json', '--', @tokens);
    my $status = $attempt->run(\@args);
    if ($status == 0) {
        $attempt->state_set('classification', 'completed');
        $attempt->runlog_note('VASP completed');
        $attempt->advance('publish', []);
    } else {
        $attempt->fail('vasp.failed', "VASP did not complete (status $status)", 0);
    }
    return 0;
}

sub step_publish {
    my ($attempt) = @_;
    my $prefix = $attempt->parameter('data_prefix', 'vasp');
    my $data_dir = env_or('HTTK_WORKFLOW_DATA_DIR', '');
    for my $name (@collect) {
        next unless -f $name;
        $attempt->put($name, "$prefix/$name") if $data_dir ne '';
    }
    $attempt->runlog_note($data_dir ne '' ? 'published to transactional data' : 'kept the result in the workdir');
    $attempt->succeed();
    return 0;
}

my $runner = HttkWorkflow::Runner->new(
    workflow => 'httk.vasp.relax-perl',
    steps => [qw(prepare run publish)],
);
$runner->step(prepare => \&step_prepare)
    ->step(run => \&step_run)
    ->step(publish => \&step_publish)
    ->main();
