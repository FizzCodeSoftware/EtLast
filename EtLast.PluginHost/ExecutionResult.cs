namespace FizzCode.EtLast.PluginHost
{
    public enum ExecutionResult
    {
        Success = 0,

        PluginFailed = 101, // at least one plugin failed but none of them requested termination
        PluginFailedAndExecutionTerminated = 102, // at least one plugin failed and requested termination

        ConfigurationError = 1001,
        WrongArguments = 1002,
    }
}