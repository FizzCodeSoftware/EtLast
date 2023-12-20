namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ConsoleHostFluent
{
    public static T ClearReferenceAssemblyFolder<T>(this T host)
        where T : ConsoleHost
    {
        host.ReferenceAssemblyFolders.Clear();
        return host;
    }

    public static T AddReferenceAssemblyFolder<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.ReferenceAssemblyFolders.Add(path);
        return host;
    }

    public static T UseHostArgumentsFolder<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.HostArgumentsFolder = path;
        return host;
    }

    public static T UseModulesFolder<T>(this T host, string path)
        where T : ConsoleHost
    {
        host.ModulesFolder = path;
        return host;
    }

    public static T SetModuleCompilationMode<T>(this T host, ModuleCompilationMode moduleCompilationMode)
        where T : ConsoleHost
    {
        host.ModuleCompilationMode = moduleCompilationMode;
        return host;
    }
}