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
