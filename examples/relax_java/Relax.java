import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static java.lang.System.getenv;

/** One minimal three-step VASP relaxation using the native Java SDK. */
public final class Relax {
    private static final String[] COLLECT = {
        "INCAR", "KPOINTS", "OUTCAR", "CONTCAR", "OSZICAR", "vasprun.xml", "vasp-run-report.json"
    };

    private Relax() {
    }

    private static String envOr(String name, String fallback) {
        String value = getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static int stageInput(
            HttkWorkflow.Attempt attempt,
            String jobDir,
            String parameter,
            String fallback,
            String destination) throws IOException {
        String relative = attempt.parameter(parameter, fallback).orElse(fallback);
        Path source = Path.of(jobDir, relative);
        if (!Files.isRegularFile(source)) {
            return 0;
        }
        Files.copy(source, Path.of(destination), StandardCopyOption.REPLACE_EXISTING);
        return 1;
    }

    private static int prepare(HttkWorkflow.Attempt attempt) throws IOException {
        String jobDir = envOr("HTTK_WORKFLOW_JOB_DIR", ".");
        if (stageInput(attempt, jobDir, "poscar", "files/POSCAR", "POSCAR") <= 0) {
            attempt.fail("vasp.input_missing", "the starting structure is not in this payload", false);
            return 0;
        }
        stageInput(attempt, jobDir, "incar", "files/INCAR", "INCAR");
        attempt.runlogNote("prepared a relaxation");
        attempt.advance("run");
        return 0;
    }

    private static int run(HttkWorkflow.Attempt attempt) {
        String fromParameter = attempt.parameter("vasp_command", "").orElse("");
        String command = attempt.setting("vasp.command", fromParameter).orElse("");
        if (command.trim().isEmpty()) {
            attempt.fail(
                    "vasp.command_missing",
                    "no VASP command is configured: set it with "
                            + "httk workflow workspace settings set --key vasp.command --value '...' WORKSPACE, or set "
                            + "HTTK_VASP_COMMAND, or give the job a vasp_command parameter",
                    false);
            return 0;
        }

        String timeout = attempt.parameter("timeout", "86400").orElse("86400");
        String[] tokens = command.trim().split("\\s+");
        String[] args = new String[5 + tokens.length];
        args[0] = "--timeout";
        args[1] = timeout;
        args[2] = "--report";
        args[3] = "vasp-run-report.json";
        args[4] = "--";
        System.arraycopy(tokens, 0, args, 5, tokens.length);

        int status = attempt.run(args);
        if (status == 0) {
            attempt.stateSet("classification", "completed");
            attempt.runlogNote("VASP completed");
            attempt.advance("publish");
        } else {
            attempt.fail("vasp.failed", "VASP did not complete (status " + status + ")", false);
        }
        return 0;
    }

    private static int publish(HttkWorkflow.Attempt attempt) {
        String prefix = attempt.parameter("data_prefix", "vasp").orElse("vasp");
        String dataDir = envOr("HTTK_WORKFLOW_DATA_DIR", "");
        for (String name : COLLECT) {
            if (Files.isRegularFile(Path.of(name)) && !dataDir.isEmpty()) {
                attempt.put(name, prefix + "/" + name);
            }
        }
        attempt.runlogNote(dataDir.isEmpty() ? "kept the result in the workdir" : "published to transactional data");
        attempt.succeed();
        return 0;
    }

    public static void main(String[] args) {
        new HttkWorkflow.Runner("httk.vasp.relax-java", Arrays.asList("prepare", "run", "publish"))
                .step("prepare", Relax::prepare)
                .step("run", Relax::run)
                .step("publish", Relax::publish)
                .main(args);
    }
}
