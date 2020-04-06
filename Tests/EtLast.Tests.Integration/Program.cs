namespace Mnb.KofirVIR.EtlHost
{
    using System;
    using System.Data.Common;
    using FizzCode.EtLast.PluginHost;

    public static class Program
    {
        private static void Main(string[] args)
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);

            AppDomain.MonitoringIsEnabled = true;

            CommandLineHandler.Run("EtLast Integration Tests", args);

#if DEBUG
            if (args?.Length > 0)
            {
                Console.WriteLine();
                Console.WriteLine("done, press any key to continue");
                Console.ReadKey();
            }
#endif
        }
    }
}