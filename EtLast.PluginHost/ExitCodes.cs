namespace FizzCode.EtLast.PluginHost
{
    public enum ExitCode
    {
        ERR_NO_ERROR = 0,

        // plugin errors: 100 < x < 200
        ERR_AT_LEAST_ONE_PLUGIN_FAILED = 101,
        ERR_EXECUTION_TERMINATED = 102,

        // configuration errors, no retry should happen. 1000 < x < 2000
        ERR_NO_CONFIG = 1001,
        ERR_WRONG_ARGUMENTS = 1002,
        ERR_NOTHING_TO_EXECUTE = 1003,
    }
}