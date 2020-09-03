namespace Mnb.KofirVIR.EtlHost
{
    using System;
    using System.Data.Common;
    using System.Diagnostics;
    using FizzCode.EtLast.PluginHost;

    public static class Program
    {
        private static int Main(string[] args)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);

            AppDomain.MonitoringIsEnabled = true;

            var result = CommandLineHandler.Run("EtLast Integration Tests", args);

#if DEBUG
            if (args?.Length > 0 && Debugger.IsAttached)
            {
                Console.WriteLine();
                Console.WriteLine("done, press any key to continue");
                Console.ReadKey();
            }
#endif

            return (int)result;
        }
    }
}