namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class AbstractCommandServiceFluent
{
    public static T AddCustomAction<T>(this T commandService, Action<ICommandService> action)
        where T : AbstractCommandService
    {
        commandService.CustomActions.Add(action);
        return commandService;
    }

    public static T AddCommandListener<T>(this T commandService, Func<ICommandService, ICommandListener> listenerCreator)
        where T : AbstractCommandService
    {
        commandService.CommandListenerCreators.Add(listenerCreator);
        return commandService;
    }

    public static T AddCommandListener<T>(this T commandService, Type listenerType)
    where T : AbstractCommandService
    {
        commandService.CommandListenerCreators.Add(_ => (ICommandListener)Activator.CreateInstance(listenerType));
        return commandService;
    }

    public static T RegisterEtlContextListener<T>(this T commandService, Func<IEtlContext, IEtlContextListener> listenerCreator)
        where T : AbstractCommandService
    {
        commandService.EtlContextListenerCreators.Add(listenerCreator);
        return commandService;
    }

    public static T SetAlias<T>(this T commandService, string alias, string commandLine)
        where T : AbstractCommandService
    {
        commandService.CommandAliases[alias] = commandLine;
        return commandService;
    }

    public static T DisableModuleLogging<T>(this T commandService)
        where T : AbstractCommandService
    {
        commandService.ModuleLoggingEnabled = false;
        return commandService;
    }

    public static T DisableServiceLogging<T>(this T commandService)
        where T : AbstractCommandService
    {
        commandService.ServiceLoggingEnabled = false;
        return commandService;
    }

    public static T IfInstanceIs<T>(this T commandService, string instanceName, Func<T, T> action)
        where T : AbstractCommandService
    {
        if (Environment.MachineName.Equals(instanceName, StringComparison.InvariantCultureIgnoreCase))
            return action.Invoke(commandService);

        return commandService;
    }

    public static T IfDebuggerAttached<T>(this T commandService, Func<T, T> action)
        where T : AbstractCommandService
    {
        if (Debugger.IsAttached)
            return action.Invoke(commandService);

        return commandService;
    }

    /// <summary>
    /// .NET allows maximum 10 minute long transactions, but each <see cref="ICommandService"/> automatically apply a hack with 4 hours, which can be overwritten by using this method.
    /// </summary>
    /// <param name="commandService"></param>
    /// <param name="maxTimeout"></param>
    /// <returns></returns>
    public static T SetMaxTransactionTimeout<T>(this T commandService, TimeSpan maxTimeout)
        where T : AbstractCommandService
    {
        commandService.MaxTransactionTimeout = maxTimeout;
        return commandService;
    }
}