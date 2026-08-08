/*
 * Native httk-workflow C++ authoring SDK.
 *
 * This is a C++17 RAII wrapper over the native C SDK. Compile and link
 * native/c/httk_workflow.c separately; this header only wraps its declarations.
 */

#ifndef HTTK_WORKFLOW_HPP
#define HTTK_WORKFLOW_HPP

#include "../c/httk_workflow.h"

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <optional>

namespace httk::workflow {

class BridgeError : public std::runtime_error {
public:
    explicit BridgeError(int status)
        : std::runtime_error("httk-workflow bridge call refused (status " + std::to_string(status) + ")"),
          status_(status) {}

    int status() const noexcept { return status_; }

private:
    int status_;
};

template <int (*F)()>
int guarded() noexcept {
    try {
        return F();
    } catch (const BridgeError& error) {
        std::fprintf(stderr, "httk-workflow: C++ handler exception: %s\n", error.what());
    } catch (const std::exception& error) {
        std::fprintf(stderr, "httk-workflow: C++ handler exception: %s\n", error.what());
    } catch (...) {
        std::fputs("httk-workflow: C++ handler exception: unknown exception\n", stderr);
    }
    return 1;
}

class Runner {
public:
    // Handlers must be plain C-compatible functions. Capturing lambdas and
    // std::function objects cannot be converted to the C function-pointer ABI.
    using Handler = int (*)();

    explicit Runner(std::string workflow) : workflow_(std::move(workflow)) {}

    Runner& add_step(std::string name, Handler handler) {
        steps_.emplace_back(std::move(name), handler);
        return *this;
    }

    int main(int argc, char** argv) const {
        std::vector<httk_workflow_step> c_steps;
        c_steps.reserve(steps_.size());
        for (const auto& step : steps_) {
            c_steps.push_back({step.first.c_str(), step.second});
        }
        const auto status = httk_workflow_runner(workflow_.c_str(), c_steps.data(), c_steps.size());
        return status == HTTK_WORKFLOW_OK ? httk_workflow_main(argc, argv) : status;
    }

    int describe() const {
        std::vector<httk_workflow_step> c_steps;
        c_steps.reserve(steps_.size());
        for (const auto& step : steps_) {
            c_steps.push_back({step.first.c_str(), step.second});
        }
        const auto status = httk_workflow_runner(workflow_.c_str(), c_steps.data(), c_steps.size());
        if (status != HTTK_WORKFLOW_OK) return status;
        httk_workflow_describe();
        return HTTK_WORKFLOW_OK;
    }

private:
    std::string workflow_;
    std::vector<std::pair<std::string, Handler>> steps_;
};

class Attempt {
public:
    using Arguments = std::vector<std::string>;

    static int invoke(const Arguments& arguments) {
        const auto pointers = c_arguments(arguments);
        return httk_workflow_invoke(nullptr, pointers.data());
    }

    static std::optional<std::string> invoke_capture(const Arguments& arguments) {
        const auto pointers = c_arguments(arguments);
        char* output = nullptr;
        const int status = httk_workflow_invoke(&output, pointers.data());
        return read_result(output, status);
    }

    static std::optional<std::string> context() { return read(httk_workflow_context, nullptr); }
    static std::optional<std::string> context(const std::string& field) {
        return read(httk_workflow_context, field.c_str());
    }

    static std::optional<std::string> parameter(const std::string& name) {
        return named_read(httk_workflow_parameter, name, nullptr);
    }
    static std::optional<std::string> parameter(const std::string& name, const std::string& fallback) {
        return named_read(httk_workflow_parameter, name, fallback.c_str());
    }
    static std::optional<std::string> setting(const std::string& name) {
        return named_read(httk_workflow_setting, name, nullptr);
    }
    static std::optional<std::string> setting(const std::string& name, const std::string& fallback) {
        return named_read(httk_workflow_setting, name, fallback.c_str());
    }
    static std::optional<std::string> environment(const std::string& name) {
        return named_read(httk_workflow_environment, name, nullptr);
    }
    static std::optional<std::string> environment(const std::string& name, const std::string& fallback) {
        return named_read(httk_workflow_environment, name, fallback.c_str());
    }
    static std::optional<std::string> state_get(const std::string& name) {
        return read(httk_workflow_state_get, name.c_str());
    }
    static std::optional<std::string> declaration(const std::string& name) {
        return read(httk_workflow_declaration, name.c_str());
    }
    static std::optional<std::string> children() { return read(httk_workflow_children, nullptr); }
    static std::optional<std::string> children(const std::string& selection) {
        return read(httk_workflow_children, selection.c_str());
    }
    static std::optional<std::string> child(const std::string& label, const std::string& field) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_workflow_child(label.c_str(), field.c_str(), &status);
        return read_result(output, status);
    }

    static int state_set(const std::string& name, const std::string& value) {
        return httk_workflow_state_set(name.c_str(), value.c_str());
    }
    static int state_delete(const std::string& name) { return httk_workflow_state_delete(name.c_str()); }
    static int state_merge(const Arguments& assignments) {
        return with_arguments(assignments, [](const char* const* args) {
            return httk_workflow_state_merge(args);
        });
    }
    static int declare(const std::string& name, const std::string& document_file) {
        return httk_workflow_declare(name.c_str(), document_file.c_str());
    }
    static int runlog_note(const std::string& message) { return httk_workflow_runlog_note(message.c_str()); }
    static int runlog_headline(const std::string& message) {
        return httk_workflow_runlog_headline(message.c_str());
    }
    static int runlog_append(const std::string& message, const Arguments& files = {}) {
        return with_arguments(files, [&](const char* const* args) {
            return httk_workflow_runlog_append(message.c_str(), args);
        });
    }
    static int log(const std::string& level, const std::string& message) {
        return httk_workflow_log(level.c_str(), message.c_str());
    }

    static std::optional<std::string> put(const std::string& source, const std::string& destination) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_workflow_put(source.c_str(), destination.c_str(), &status);
        return read_result(output, status);
    }
    static std::optional<std::string> remove(const std::string& destination, bool missing_ok = false) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_workflow_remove(destination.c_str(), missing_ok ? 1 : 0, &status);
        return read_result(output, status);
    }
    static std::string spawn(const std::string& label, const Arguments& arguments = {}) {
        int status = HTTK_WORKFLOW_OK;
        auto args = c_arguments(arguments);
        char* output = httk_workflow_spawn(label.c_str(), args.empty() ? nullptr : args.data(), &status);
        return required_result(output, status);
    }

    static int advance(const std::string& next_step, const Arguments& arguments = {}) {
        return with_arguments(arguments, [&](const char* const* args) {
            return httk_workflow_advance(next_step.c_str(), args);
        });
    }
    static int gather(const std::string& next_step, const Arguments& arguments = {}) {
        return with_arguments(arguments, [&](const char* const* args) {
            return httk_workflow_gather(next_step.c_str(), args);
        });
    }
    static int succeed() { return httk_workflow_succeed(); }
    static int fail(const std::string& code, const std::string& message, const Arguments& arguments = {}) {
        return with_arguments(arguments, [&](const char* const* args) {
            return httk_workflow_fail(code.c_str(), message.c_str(), args);
        });
    }
    static int retry(const std::string& reason) { return httk_workflow_retry(reason.c_str()); }
    static int pause(const std::string& reason) { return httk_workflow_pause(reason.c_str()); }
    static int batch() { return httk_workflow_batch(); }

    static std::optional<std::string> job_prepare(
        const std::string& destination, const std::string& spec_file) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_workflow_job_prepare(destination.c_str(), spec_file.c_str(), &status);
        return read_result(output, status);
    }
    static std::optional<std::string> workdir_apply(const std::string& spec_file) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_workflow_workdir_apply(spec_file.c_str(), &status);
        return read_result(output, status);
    }
    static int run(const Arguments& arguments) {
        return with_arguments(arguments, [](const char* const* args) {
            return httk_workflow_run(args);
        });
    }
    static std::optional<std::string> calc(const std::string& expression) {
        int status = HTTK_WORKFLOW_OK;
        char* output = httk_calc(expression.c_str(), &status);
        return read_result(output, status);
    }
    static int template_render(
        const std::string& template_file, const std::string& output, const std::string& values_file) {
        return httk_template_render(template_file.c_str(), output.c_str(), values_file.c_str());
    }
    static int compress(const Arguments& arguments) {
        return with_arguments(arguments, [](const char* const* args) {
            return httk_compress(args);
        });
    }
    static int decompress(const Arguments& arguments) {
        return with_arguments(arguments, [](const char* const* args) {
            return httk_decompress(args);
        });
    }

private:
    struct FreeCString {
        void operator()(char* value) const noexcept { std::free(value); }
    };

    using CString = std::unique_ptr<char, FreeCString>;
    using ReadFunction = char* (*)(const char*, int*);

    static std::vector<const char*> c_arguments(const Arguments& arguments) {
        std::vector<const char*> result;
        result.reserve(arguments.size() + 1);
        for (const auto& argument : arguments) result.push_back(argument.c_str());
        result.push_back(nullptr);
        return result;
    }

    template <typename Function>
    static int with_arguments(const Arguments& arguments, Function function) {
        const auto pointers = c_arguments(arguments);
        return function(arguments.empty() ? nullptr : pointers.data());
    }

    static std::optional<std::string> read_result(char* output, int status) {
        CString value(output);
        if (status == HTTK_WORKFLOW_ABSENT) return std::nullopt;
        if (status != HTTK_WORKFLOW_OK) throw BridgeError(status);
        return value ? std::optional<std::string>(std::string(value.get())) : std::nullopt;
    }

    static std::string required_result(char* output, int status) {
        CString value(output);
        if (status != HTTK_WORKFLOW_OK) throw BridgeError(status);
        if (!value) throw BridgeError(HTTK_WORKFLOW_REFUSED);
        return std::string(value.get());
    }

    static std::optional<std::string> read(ReadFunction function, const char* argument) {
        int status = HTTK_WORKFLOW_OK;
        char* output = function(argument, &status);
        return read_result(output, status);
    }

    static std::optional<std::string> named_read(
        char* (*function)(const char*, const char*, int*), const std::string& name, const char* fallback) {
        int status = HTTK_WORKFLOW_OK;
        return read_result(function(name.c_str(), fallback, &status), status);
    }
};

}  // namespace httk::workflow

#endif  // HTTK_WORKFLOW_HPP
