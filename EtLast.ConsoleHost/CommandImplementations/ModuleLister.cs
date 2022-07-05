namespace FizzCode.EtLast.ConsoleHost;

internal static class ModuleLister
{
    public static void ListModules(Host host)
    {
        var moduleNames = GetAllModules(host.ModulesFolder);
        host.Logger.Information("available modules: {ModuleNames}", moduleNames);
    }

    public static List<string> GetAllModules(string folder)
    {
        var moduleFolders = Directory.GetDirectories(folder)
             .Where(moduleFolder =>
             {
                 var moduleConfigFileName = Path.Combine(moduleFolder, "Startup.cs");
                 return File.Exists(moduleConfigFileName);
             }).OrderBy(x => x)
            .ToList();

        return moduleFolders.ConvertAll(Path.GetFileName);
    }
}