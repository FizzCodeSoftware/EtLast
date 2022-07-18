namespace FizzCode.EtLast.ConsoleHost;

public static class IHostBuilderExtensions
{
    public static IHostBuilder HandleCommandLineArgs(this IHostBuilder builder, string[] startupArguments)
    {
        builder.Result.CommandLineArgs = startupArguments;
        return builder;
    }

    public static IHostBuilder UseCommandLineListener(this IHostBuilder builder, Func<ArgumentCollection, ICommandLineListener> listener)
    {
        builder.Result.CommandLineListenerCreators.Add(listener);
        return builder;
    }

    public static IHostBuilder RegisterEtlContextListener(this IHostBuilder builder, Func<IEtlSession, IEtlContextListener> listenerCreator)
    {
        builder.Result.EtlContextListeners.Add(listenerCreator);
        return builder;
    }

    public static IHostBuilder ClearReferenceAssemblyFolder(this IHostBuilder builder)
    {
        builder.Result.ReferenceAssemblyFolders.Clear();
        return builder;
    }

    public static IHostBuilder AddReferenceAssemblyFolder(this IHostBuilder builder, string path)
    {
        builder.Result.ReferenceAssemblyFolders.Add(path);
        return builder;
    }

    public static IHostBuilder UseHostArgumentsFolder(this IHostBuilder builder, string path)
    {
        builder.Result.HostArgumentsFolder = path;
        return builder;
    }

    public static IHostBuilder UseModulesFolder(this IHostBuilder builder, string path)
    {
        builder.Result.ModulesFolder = path;
        return builder;
    }

    public static IHostBuilder SetAlias(this IHostBuilder builder, string alias, string commandLine)
    {
        builder.Result.CommandAliases[alias] = commandLine;
        return builder;
    }

    public static IHostBuilder DisableSerilogForModules(this IHostBuilder builder)
    {
        builder.Result.SerilogForModulesEnabled = false;
        return builder;
    }

    public static IHostBuilder DisableSerilogForCommands(this IHostBuilder builder)
    {
        builder.Result.SerilogForHostEnabled = false;
        return builder;
    }

    /// <summary>
    /// .NET allows maximum 10 minute long transactions, but each <see cref="Host"/> automatically apply a hack with 4 hours, which can be overwritten by using this method.
    /// </summary>
    /// <param name="builder"></param>
    /// <param name="maxTimeout"></param>
    /// <returns></returns>
    public static IHostBuilder SetMaxTransactionTimeout(this IHostBuilder builder, TimeSpan maxTimeout)
    {
        builder.Result.MaxTransactionTimeout = maxTimeout;
        return builder;
    }
}