using FizzCode.EtLast.Hosting;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class AbstractCommandServiceFluent
{
    public static T AddCommandListener<T>(this T commandService, Func<IArgumentCollection, ICommandListener> listenerCreator)
        where T : AbstractEtlCommandService
    {
        commandService.CommandListenerCreators.Add(listenerCreator);
        return commandService;
    }

    public static T RegisterEtlContextListener<T>(this T commandService, Func<IEtlContext, IEtlContextListener> listenerCreator)
        where T : AbstractEtlCommandService
    {
        commandService.EtlContextListeners.Add(listenerCreator);
        return commandService;
    }

    public static T SetAlias<T>(this T commandService, string alias, string commandLine)
        where T : AbstractEtlCommandService
    {
        commandService.CommandAliases[alias] = commandLine;
        return commandService;
    }

    public static T DisableSerilogForModules<T>(this T commandService)
        where T : AbstractEtlCommandService
    {
        commandService.SerilogForModulesDisabled = false;
        return commandService;
    }

    public static T DisableSerilogForCommands<T>(this T commandService)
        where T : AbstractEtlCommandService
    {
        commandService.SerilogForCommandsEnabled = false;
        return commandService;
    }

    public static T IfInstanceIs<T>(this T commandService, string instanceName, Func<T, T> action)
        where T : AbstractEtlCommandService
    {
        if (Environment.MachineName.Equals(instanceName, StringComparison.InvariantCultureIgnoreCase))
            return action.Invoke(commandService);

        return commandService;
    }

    public static T IfDebuggerAttached<T>(this T commandService, Func<T, T> action)
        where T : AbstractEtlCommandService
    {
        if (Debugger.IsAttached)
            return action.Invoke(commandService);

        return commandService;
    }

    /// <summary>
    /// .NET allows maximum 10 minute long transactions, but each <see cref="IEtlCommandService"/> automatically apply a hack with 4 hours, which can be overwritten by using this method.
    /// </summary>
    /// <param name="commandService"></param>
    /// <param name="maxTimeout"></param>
    /// <returns></returns>
    public static T SetMaxTransactionTimeout<T>(this T commandService, TimeSpan maxTimeout)
        where T : AbstractEtlCommandService
    {
        commandService.MaxTransactionTimeout = maxTimeout;
        return commandService;
    }
}