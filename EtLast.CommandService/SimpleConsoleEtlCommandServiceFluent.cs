namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SimpleConsoleEtlCommandServiceFluent
{
    public static T ClearReferenceAssemblyDirectories<T>(this T host)
        where T : CommandService
    {
        host.ReferenceAssemblyDirectories.Clear();
        return host;
    }

    public static T AddReferenceAssemblyDirectory<T>(this T host, string path)
        where T : CommandService
    {
        host.ReferenceAssemblyDirectories.Add(path);
        return host;
    }

    public static T UseHostArgumentsDirectory<T>(this T host, string path)
        where T : CommandService
    {
        host.ServiceArgumentsDirectory = path;
        return host;
    }

    public static T UseModulesDirectory<T>(this T host, string path)
        where T : CommandService
    {
        host.ModulesDirectory = path;
        return host;
    }

    public static T SetModuleCompilationMode<T>(this T host, ModuleCompilationMode moduleCompilationMode)
        where T : CommandService
    {
        host.ModuleCompilationMode = moduleCompilationMode;
        return host;
    }

    public delegate void SessionBuilderAction(ISessionBuilder builder, IArgumentCollection sessionArguments);

    public static T ConfigureSession<T>(this T host, SessionBuilderAction builderAction)
        where T : CommandService
    {
        host.SessionConfigurator = builderAction;
        return host;
    }
}