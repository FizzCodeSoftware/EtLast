namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class IHostBuilderExtensions
{
    public static IHostBuilder UseCommandListener(this IHostBuilder builder, Func<IArgumentCollection, ICommandListener> listenerCreator)
    {
        builder.Result.CommandListenerCreators.Add(listenerCreator);
        return builder;
    }

    public static IHostBuilder RegisterEtlContextListener(this IHostBuilder builder, Func<IEtlContext, IEtlContextListener> listenerCreator)
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
        builder.Result.SerilogForModulesDisabled = false;
        return builder;
    }

    public static IHostBuilder DisableSerilogForCommands(this IHostBuilder builder)
    {
        builder.Result.SerilogForHostEnabled = false;
        return builder;
    }

    public static IHostBuilder IfInstanceIs(this IHostBuilder builder, string instanceName, Func<IHostBuilder, IHostBuilder> builderAction)
    {
        if (Environment.MachineName.Equals(instanceName, StringComparison.InvariantCultureIgnoreCase))
            return builderAction.Invoke(builder);

        return builder;
    }

    public static IHostBuilder IfDebuggerAttached(this IHostBuilder builder, Func<IHostBuilder, IHostBuilder> builderAction)
    {
        if (Debugger.IsAttached)
            return builderAction.Invoke(builder);

        return builder;
    }

    /// <summary>
    /// .NET allows maximum 10 minute long transactions, but each <see cref="IHost"/> automatically apply a hack with 4 hours, which can be overwritten by using this method.
    /// </summary>
    /// <param name="builder"></param>
    /// <param name="maxTimeout"></param>
    /// <returns></returns>
    public static IHostBuilder SetMaxTransactionTimeout(this IHostBuilder builder, TimeSpan maxTimeout)
    {
        builder.Result.MaxTransactionTimeout = maxTimeout;
        return builder;
    }

    public static IHostBuilder SetModuleCompilationMode(this IHostBuilder builder, ModuleCompilationMode moduleCompilationMode)
    {
        builder.Result.ModuleCompilationMode = moduleCompilationMode;
        return builder;
    }
}