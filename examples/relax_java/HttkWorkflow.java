import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A java.base-only bridge client for httk-workflow runners. */
public final class HttkWorkflow {
    public static final int API_VERSION = 2;
    public static final int OK = 0;
    public static final int ABSENT = 1;
    public static final int REFUSED = 2;

    private static final String PYTHON = "HTTK_WORKFLOW_PYTHON";
    private static final String BRIDGE = "httk.workflow._shell_bridge";
    private static final String DESCRIBE = "HTTK_WORKFLOW_DESCRIBE";
    private static final String RUNNER_WORKFLOW = "HTTK_WORKFLOW_RUNNER_WORKFLOW";
    private static final String RUNNER_STEPS = "HTTK_WORKFLOW_RUNNER_STEPS";
    private static final String STEP = "HTTK_WORKFLOW_STEP";
    private static final String CONTROL = "HTTK_WORKFLOW_CONTROL_DIR";
    private static final String ABORT_EXCEPTION = "JavaError";
    private static final DateTimeFormatter LOG_TIME = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    private HttkWorkflow() {
    }

    /** An error reaching the bridge or a bridge call that was refused. */
    public static final class BridgeError extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String kind;
        private final int status;

        private BridgeError(String kind, int status, String message, Throwable cause) {
            super(message, cause);
            this.kind = kind;
            this.status = status;
        }

        public String kind() {
            return kind;
        }

        public int status() {
            return status;
        }

        private static BridgeError pythonUnset() {
            return new BridgeError("PythonUnset", REFUSED,
                    PYTHON + " is not set by the workflow manager", null);
        }

        private static BridgeError spawn(IOException error) {
            return new BridgeError("Spawn", REFUSED,
                    "could not start the httk-workflow bridge: " + error.getMessage(), error);
        }

        private static BridgeError spawn(InterruptedException error) {
            Thread.currentThread().interrupt();
            return new BridgeError("Spawn", REFUSED,
                    "interrupted while waiting for the httk-workflow bridge", error);
        }

        private static BridgeError refused(int status) {
            return new BridgeError("Refused", status,
                    "the httk-workflow bridge refused the call", null);
        }
    }

    /** Optional arguments for {@link Attempt#gather(String, Gather)}. */
    public static final class Gather {
        private String when;
        private Long count;
        private String onImpossible;
        private Long priority;

        public Gather when(String value) {
            when = value;
            return this;
        }

        public Gather count(long value) {
            count = value;
            return this;
        }

        public Gather onImpossible(String value) {
            onImpossible = value;
            return this;
        }

        public Gather priority(long value) {
            priority = value;
            return this;
        }
    }

    /** A workflow runner with one throwing-capable handler per declared step. */
    public static final class Runner {
        @FunctionalInterface
        public interface Handler {
            int handle(Attempt attempt) throws Exception;
        }

        private final String workflow;
        private final List<String> stepNames;
        private final Map<String, Handler> handlers = new LinkedHashMap<>();

        public Runner(String workflow, String... steps) {
            this(workflow, Arrays.asList(steps));
        }

        public Runner(String workflow, List<String> steps) {
            this.workflow = workflow;
            this.stepNames = new ArrayList<>(steps);
        }

        public Runner step(String name, Handler handler) {
            handlers.put(name, handler);
            return this;
        }

        public String description() {
            List<String> names = new ArrayList<>(stepNames);
            Collections.sort(names);
            List<String> quoted = new ArrayList<>();
            for (String name : names) {
                quoted.add("\"" + name + "\"");
            }
            return "{\"format\": \"httk-workflow-runner-description\", \"format_version\": 2, "
                    + "\"steps\": [" + String.join(", ", quoted) + "], \"workflow\": \"" + workflow + "\"}\n";
        }

        /** Validates, describes, or dispatches this runner and exits the process. */
        public void main(String[] args) {
            System.exit(dispatch(args == null ? new String[0] : args));
        }

        public void main() {
            main(new String[0]);
        }

        private int dispatch(String[] args) {
            if (!validate()) {
                return REFUSED;
            }
            boolean describe = false;
            for (String arg : args) {
                if ("--describe".equals(arg)) {
                    describe = true;
                    break;
                }
            }
            if ("1".equals(System.getenv(DESCRIBE))) {
                describe = true;
            }
            if (describe) {
                System.out.print(description());
                return OK;
            }

            Attempt attempt = new Attempt();
            String joinedSteps = String.join("\n", stepNames);
            setEnvironment(RUNNER_WORKFLOW, workflow);
            setEnvironment(RUNNER_STEPS, joinedSteps);

            BridgeResult begin;
            try {
                begin = attempt.capture("begin");
            } catch (BridgeError error) {
                return REFUSED;
            }
            if (begin.status != OK) {
                return REFUSED;
            }
            String step = begin.output;
            setEnvironment(STEP, step);
            if (attempt.outcomePublished()) {
                return OK;
            }

            Handler handler = handlers.get(step);
            if (handler == null) {
                try {
                    return attempt.command("fail-unknown-step") == OK ? OK : REFUSED;
                } catch (BridgeError error) {
                    return REFUSED;
                }
            }

            try {
                int status = handler.handle(attempt);
                if (status != OK) {
                    abort(attempt, step, status, step + " exited with status " + status);
                    return status;
                }
            } catch (Throwable error) {
                String message = error.getMessage();
                if (message == null || message.isEmpty()) {
                    message = error.toString();
                }
                message = stripTrailingNewlines(message);
                abort(attempt, step, REFUSED, message);
                return REFUSED;
            }

            try {
                if (!attempt.outcomePublished() && attempt.command("fail-no-outcome") != OK) {
                    return REFUSED;
                }
                return attempt.command("environment-log") == OK ? OK : REFUSED;
            } catch (BridgeError error) {
                return REFUSED;
            }
        }

        private void abort(Attempt attempt, String step, int status, String message) {
            String finalMessage = message == null || message.isEmpty()
                    ? step + " exited with status " + status
                    : stripTrailingNewlines(message);
            try {
                attempt.command("abort", "--exception", ABORT_EXCEPTION, "--message", finalMessage);
            } catch (BridgeError ignored) {
                // The handler's status is the useful failure when breadcrumb publication is refused.
            }
        }

        private boolean validate() {
            if (workflow == null || workflow.isEmpty() || stepNames.isEmpty()) {
                System.err.println("httk-workflow: a runner needs a workflow name and at least one step");
                return false;
            }
            if (!validName(workflow)) {
                System.err.println("httk-workflow: workflow name " + workflow + " cannot name a runner");
                return false;
            }
            for (int index = 0; index < stepNames.size(); index++) {
                String name = stepNames.get(index);
                if (!validName(name)) {
                    System.err.println("httk-workflow: step name " + name + " cannot name a Java step handler");
                    return false;
                }
                if (stepNames.subList(0, index).contains(name)) {
                    System.err.println("httk-workflow: step " + name + " is already registered on the " + workflow
                            + " runner");
                    return false;
                }
                if (!handlers.containsKey(name) || handlers.get(name) == null) {
                    System.err.println("httk-workflow: the " + workflow + " runner declares step " + name
                            + " but registers no handler");
                    return false;
                }
            }
            for (String name : handlers.keySet()) {
                if (!stepNames.contains(name)) {
                    System.err.println("httk-workflow: step " + name + " has a handler but is not declared on the "
                            + workflow + " runner");
                    return false;
                }
            }
            return true;
        }
    }

    /** The attempt API used by a step handler. */
    public static final class Attempt {
        public Attempt() {
        }

        public int invoke(String... args) {
            return command(args);
        }

        public Optional<String> context() {
            return read("context");
        }

        public Optional<String> context(String field) {
            return read("context", field);
        }

        public Optional<String> parameter(String name) {
            return read("parameter", name);
        }

        public Optional<String> parameter(String name, String fallback) {
            return read("parameter", name, "--default", fallback);
        }

        public Optional<String> setting(String name) {
            return read("setting", name);
        }

        public Optional<String> setting(String name, String fallback) {
            return read("setting", name, "--default", fallback);
        }

        public Optional<String> environment(String name) {
            return read("environment", name);
        }

        public Optional<String> environment(String name, String fallback) {
            return read("environment", name, "--default", fallback);
        }

        public Optional<String> stateGet(String name) {
            return read("state-get", name);
        }

        public int stateSet(String name, String value) {
            return command("state-set", name, value);
        }

        public int stateDelete(String name) {
            return command("state-delete", name);
        }

        public int stateMerge(String... assignments) {
            return commandWithTail("state-merge", assignments);
        }

        public Optional<String> declaration(String name) {
            return read("declaration", name);
        }

        public int declare(String name, String documentFile) {
            return command("declare", name, documentFile);
        }

        public int runlogNote(String message) {
            return command("runlog", "note", message);
        }

        public int runlogHeadline(String message) {
            return command("runlog", "headline", message);
        }

        public int runlogAppend(String message, String... files) {
            List<String> args = new ArrayList<>();
            args.add("runlog");
            args.add("files");
            args.add(message);
            if (files != null) {
                args.addAll(Arrays.asList(files));
            }
            return command(args);
        }

        /** Writes the one local, timestamped stderr log line; it does not use the bridge. */
        public void log(String level, String message) {
            System.err.println(LOG_TIME.format(Instant.now()) + " [" + level + "] " + message);
        }

        public Optional<String> put(String source, String destination) {
            return read("put", source, destination);
        }

        public Optional<String> remove(String destination, boolean missingOk) {
            return missingOk
                    ? read("remove", destination, "--missing-ok")
                    : read("remove", destination);
        }

        public Optional<String> spawn(String label, String... args) {
            return readWithTail("spawn", label, args);
        }

        public Optional<String> children() {
            return read("children");
        }

        public Optional<String> children(String selection) {
            return read("children", selection);
        }

        public Optional<String> child(String label, String field) {
            return read("child", label, field);
        }

        public int advance(String nextStep, String... args) {
            return commandWithTail("advance", nextStep, args);
        }

        public int gather(String nextStep, Gather options) {
            List<String> args = new ArrayList<>();
            args.add("gather");
            args.add(nextStep);
            if (options != null) {
                if (options.when != null) {
                    args.add("--when");
                    args.add(options.when);
                }
                if (options.count != null) {
                    args.add("--count");
                    args.add(Long.toString(options.count));
                }
                if (options.onImpossible != null) {
                    args.add("--on-impossible");
                    args.add(options.onImpossible);
                }
                if (options.priority != null) {
                    args.add("--priority");
                    args.add(Long.toString(options.priority));
                }
            }
            return command(args);
        }

        public int succeed() {
            return command("succeed");
        }

        public int fail(String code, String message, boolean retryable) {
            return retryable
                    ? command("fail", code, message, "--retryable")
                    : command("fail", code, message);
        }

        public int retry(String reason) {
            return command("retry", reason);
        }

        public int pause(String reason) {
            return command("pause", reason);
        }

        public int batch() {
            return command("batch");
        }

        public Optional<String> jobPrepare(String destination, String specFile) {
            return read("job-prepare", destination, specFile);
        }

        public Optional<String> workdirApply(String specFile) {
            return read("workdir-apply", specFile);
        }

        public int run(String... args) {
            return commandWithTail("run", args);
        }

        public Optional<String> calc(String expression) {
            return read("calc", expression);
        }

        public int templateRender(String templateFile, String output, String valuesFile) {
            return command("template", templateFile, output, valuesFile);
        }

        public int compress(String... args) {
            return commandWithTail("compress", args);
        }

        public int decompress(String... args) {
            return commandWithTail("decompress", args);
        }

        private int command(String... args) {
            return bridge(Arrays.asList(args), false).status;
        }

        private int command(List<String> args) {
            return bridge(args, false).status;
        }

        private Optional<String> read(String... args) {
            BridgeResult result = bridge(Arrays.asList(args), true);
            if (result.status == OK) {
                return Optional.of(result.output);
            }
            if (result.status == ABSENT) {
                return Optional.empty();
            }
            throw BridgeError.refused(result.status);
        }

        private BridgeResult capture(String... args) {
            return bridge(Arrays.asList(args), true);
        }

        private int commandWithTail(String verb, String... tail) {
            return commandWithTail(verb, null, tail);
        }

        private int commandWithTail(String verb, String first, String... tail) {
            List<String> args = new ArrayList<>();
            args.add(verb);
            if (first != null) {
                args.add(first);
            }
            if (tail != null) {
                args.addAll(Arrays.asList(tail));
            }
            return command(args);
        }

        private Optional<String> readWithTail(String verb, String first, String... tail) {
            List<String> args = new ArrayList<>();
            args.add(verb);
            args.add(first);
            if (tail != null) {
                args.addAll(Arrays.asList(tail));
            }
            return read(args.toArray(new String[0]));
        }

        private boolean outcomePublished() {
            String control = System.getenv(CONTROL);
            Path directory = control == null || control.isEmpty() ? Path.of(".") : Path.of(control);
            return Files.isDirectory(directory.resolve("outcome.ready"));
        }
    }

    private static final class BridgeResult {
        private final int status;
        private final String output;

        private BridgeResult(int status, String output) {
            this.status = status;
            this.output = output;
        }
    }

    private static BridgeResult bridge(List<String> args, boolean capture) {
        String python = System.getenv(PYTHON);
        if (python == null || python.isEmpty()) {
            System.err.println("httk-workflow: " + PYTHON + " is not set by the workflow manager");
            throw BridgeError.pythonUnset();
        }
        List<String> command = new ArrayList<>();
        command.add(python);
        command.add("-m");
        command.add(BRIDGE);
        command.addAll(args);
        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.environment().putAll(Environment.VALUES);
            Process process;
            if (capture) {
                builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
                builder.redirectError(ProcessBuilder.Redirect.INHERIT);
                builder.redirectOutput(ProcessBuilder.Redirect.PIPE);
                process = builder.start();
                byte[] bytes = process.getInputStream().readAllBytes();
                int status = process.waitFor();
                String output = stripTrailingNewlines(new String(bytes, StandardCharsets.UTF_8));
                return new BridgeResult(status, output);
            }
            process = builder.inheritIO().start();
            return new BridgeResult(process.waitFor(), "");
        } catch (IOException error) {
            throw BridgeError.spawn(error);
        } catch (InterruptedException error) {
            throw BridgeError.spawn(error);
        }
    }

    private static void setEnvironment(String name, String value) {
        // ProcessBuilder is intentionally used for bridge argv, while these are
        // runner protocol variables inherited by each child bridge process.
        // Java cannot mutate its own environment, so dispatch uses a child-local
        // environment overlay through the bridge helper below.
        Environment.set(name, value);
    }

    private static boolean validName(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-')) {
                return false;
            }
        }
        return true;
    }

    private static String stripTrailingNewlines(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '\n') {
            end--;
        }
        return value.substring(0, end);
    }

    /** Process-local environment overlay used by Runner dispatch. */
    private static final class Environment {
        private static final Map<String, String> VALUES = new LinkedHashMap<>();

        private static void set(String name, String value) {
            VALUES.put(name, value);
        }
    }
}
