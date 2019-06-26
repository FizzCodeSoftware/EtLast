namespace FizzCode.EtLast.PluginHost
{
    public static class ExitCodes
    {
        public const int ERR_NO_ERROR = 0;

        // plugin errors: 100 < x < 200
        public const int ERR_AT_LEAST_ONE_PLUGIN_FAILED = 101;
        public const int ERR_EXECUTION_TERMINATED = 102;

        // configuration errors, no retry should happen. 1000 < x < 2000
        public const int ERR_NO_CONFIG = 1001;
        public const int ERR_WRONG_ARGUMENTS = 1002;
        public const int ERR_NOTHING_TO_EXECUTE = 1003;
    }
}