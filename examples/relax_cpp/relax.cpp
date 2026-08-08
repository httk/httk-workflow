/*
 * One VASP relaxation, authored in C++17: prepare, run, publish.
 *
 * The C++ SDK is a header-only RAII wrapper over the native C SDK. Every
 * Attempt call still reaches the shared shell bridge, so this runner publishes
 * the same protocol bytes as the other language SDKs.
 */

#include "httk_workflow.hpp"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <string>

namespace {

using httk::workflow::Attempt;
using httk::workflow::guarded;
namespace fs = std::filesystem;

constexpr std::array<const char*, 7> collect = {
    "INCAR", "KPOINTS", "OUTCAR", "CONTCAR", "OSZICAR", "vasprun.xml", "vasp-run-report.json",
};

bool file_exists(const fs::path& path) {
    std::error_code error;
    return fs::is_regular_file(path, error);
}

int stage_input(const fs::path& job_dir, const std::string& parameter, const std::string& fallback,
               const fs::path& destination) {
    const auto relative = Attempt::parameter(parameter, fallback);
    if (!relative) return -1;
    const auto source = job_dir / *relative;
    if (!file_exists(source)) return 0;
    std::error_code error;
    fs::copy_file(source, destination, fs::copy_options::overwrite_existing, error);
    return error ? -1 : 1;
}

int step_prepare() {
    const char* job_dir_value = std::getenv("HTTK_WORKFLOW_JOB_DIR");
    const fs::path job_dir = job_dir_value == nullptr ? "." : job_dir_value;
    if (stage_input(job_dir, "poscar", "files/POSCAR", "POSCAR") <= 0) {
        Attempt::fail("vasp.input_missing", "the starting structure is not in this payload");
        return 0;
    }
    stage_input(job_dir, "incar", "files/INCAR", "INCAR");
    Attempt::runlog_note("prepared a relaxation");
    Attempt::advance("run");
    return 0;
}

int step_run() {
    const auto from_parameter = Attempt::parameter("vasp_command", "");
    const auto command = Attempt::setting("vasp.command", from_parameter.value_or(""));
    if (!command || command->empty()) {
        Attempt::fail("vasp.command_missing",
                      "no VASP command is configured: set it with httk workflow workspace settings set "
                      "vasp.command '...', or set HTTK_VASP_COMMAND, or give the job a vasp_command parameter");
        return 0;
    }
    const auto timeout = Attempt::parameter("timeout", "86400");
    Attempt::Arguments arguments = {
        "--timeout", timeout.value_or("86400"), "--report", "vasp-run-report.json", "--"};
    std::istringstream words(*command);
    for (std::string word; words >> word;) arguments.push_back(std::move(word));
    const int status = Attempt::run(arguments);
    if (status == 0) {
        Attempt::state_set("classification", "completed");
        Attempt::runlog_note("VASP completed");
        Attempt::advance("publish");
    } else {
        Attempt::fail("vasp.failed", "VASP did not complete (status " + std::to_string(status) + ")");
    }
    return 0;
}

int step_publish() {
    const auto prefix = Attempt::parameter("data_prefix", "vasp");
    const char* data_dir = std::getenv("HTTK_WORKFLOW_DATA_DIR");
    for (const auto* name : collect) {
        if (!file_exists(name) || data_dir == nullptr || *data_dir == '\0' || !prefix) continue;
        Attempt::put(name, *prefix + "/" + name);
    }
    Attempt::runlog_note(data_dir != nullptr && *data_dir != '\0'
                             ? "published to transactional data"
                             : "kept the result in the workdir");
    Attempt::succeed();
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    httk::workflow::Runner runner("httk.vasp.relax-cpp");
    runner.add_step("prepare", guarded<&step_prepare>)
        .add_step("run", guarded<&step_run>)
        .add_step("publish", guarded<&step_publish>);
    return runner.main(argc, argv);
}
