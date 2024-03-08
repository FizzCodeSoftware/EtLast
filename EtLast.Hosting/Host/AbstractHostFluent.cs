using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class AbstractHostFluent
{
    public static T UseCommandListener<T>(this T host, Func<IArgumentCollection, ICommandListener> listenerCreator)
        where T : AbstractHost
    {
        host.CommandListenerCreators.Add(listenerCreator);
        return host;
    }

    public static T RegisterEtlContextListener<T>(this T host, Func<IEtlContext, IEtlContextListener> listenerCreator)
        where T : AbstractHost
    {
        host.EtlContextListeners.Add(listenerCreator);
        return host;
    }

    public static T SetAlias<T>(this T host, string alias, string commandLine)
        where T : AbstractHost
    {
        host.CommandAliases[alias] = commandLine;
        return host;
    }

    public static T DisableSerilogForModules<T>(this T host)
        where T : AbstractHost
    {
        host.SerilogForModulesDisabled = false;
        return host;
    }

    public static T DisableSerilogForCommands<T>(this T host)
        where T : AbstractHost
    {
        host.SerilogForHostEnabled = false;
        return host;
    }

    public static T IfInstanceIs<T>(this T host, string instanceName, Func<T, T> action)
        where T : AbstractHost
    {
        if (Environment.MachineName.Equals(instanceName, StringComparison.InvariantCultureIgnoreCase))
            return action.Invoke(host);

        return host;
    }

    public static T IfDebuggerAttached<T>(this T host, Func<T, T> action)
        where T : AbstractHost
    {
        if (Debugger.IsAttached)
            return action.Invoke(host);

        return host;
    }

    /// <summary>
    /// .NET allows maximum 10 minute long transactions, but each <see cref="IEtlHost"/> automatically apply a hack with 4 hours, which can be overwritten by using this method.
    /// </summary>
    /// <param name="host"></param>
    /// <param name="maxTimeout"></param>
    /// <returns></returns>
    public static T SetMaxTransactionTimeout<T>(this T host, TimeSpan maxTimeout)
        where T : AbstractHost
    {
        host.MaxTransactionTimeout = maxTimeout;
        return host;
    }
}