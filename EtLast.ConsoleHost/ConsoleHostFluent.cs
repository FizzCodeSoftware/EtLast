namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ConsoleHostFluent
{
    public static T ClearReferenceAssemblyDirectories<T>(this T host)
        where T : ConsoleHost
    {
        host.ReferenceAssemblyDirectories.Clear();
        return host;
    }

    public static T AddReferenceAssemblyDirectory<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.ReferenceAssemblyDirectories.Add(path);
        return host;
    }

    public static T UseHostArgumentsDirectory<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.HostArgumentsDirectory = path;
        return host;
    }

    public static T UseModulesDirectory<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.ModulesDirectory = path;
        return host;
    }

    public static T SetModuleCompilationMode<T>(this T host, ModuleCompilationMode moduleCompilationMode)
        where T : ConsoleHost
    {
        host.ModuleCompilationMode = moduleCompilationMode;
        return host;
    }

    public delegate void SessionBuilderAction(ISessionBuilder builder, IArgumentCollection sessionArguments);

    public static T ConfigureSession<T>(this T host, SessionBuilderAction builderAction)
        where T : ConsoleHost
    {
        host.SessionConfigurator = builderAction;
        return host;
    }
}