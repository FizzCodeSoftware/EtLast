namespace FizzCode.EtLast.PluginHost
{
    public enum ExecutionResult
    {
        Success = 0,

        ModuleLoadError = 101,
        ModuleConfigurationError = 102,

        PluginFailed = 201, // at least one plugin failed but none of them requested termination
        PluginFailedAndExecutionTerminated = 202, // at least one plugin failed and requested termination

        UnexpectedError = 666,

        HostConfigurationError = 1001,
        HostArgumentError = 1002,
    }
}