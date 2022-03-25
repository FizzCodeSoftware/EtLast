namespace FizzCode.EtLast.ConsoleHost;

public enum ExecutionResult
{
    Success = 0,

    ModuleLoadError = 101,

    ExecutionFailed = 201, // at least one task failed

    UnexpectedError = 666,

    HostConfigurationError = 1001,
    HostArgumentError = 1002,
}
