namespace FizzCode.EtLast.ConsoleHost;

public static class IHostBuilderExtensions
{
    public static IHostBuilder HandleCommandLineArgs(this IHostBuilder builder, string[] startupArguments)
    {
        builder.Result.CommandLineArgs = startupArguments;
        return builder;
    }

    public static IHostBuilder UseCommandLineListener(this IHostBuilder builder, ICommandLineListener listener)
    {
        builder.Result.CommandLineListeners.Add(listener);
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
        builder.Result.SerilogForCommandsEnabled = false;
        return builder;
    }
}