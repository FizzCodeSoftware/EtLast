namespace FizzCode.EtLast;

internal static class ModuleLister
{
    public static void ListModules(CommandService host)
    {
        var moduleNames = GetAllModules(host.ModulesDirectory);
        host.Logger.Information("available modules: {ModuleNames}", moduleNames);
    }

    public static List<string> GetAllModules(string directory)
    {
        var moduleDirectories = Directory.GetDirectories(directory)
             .Where(dir =>
             {
                 var moduleConfigFileName = Path.Combine(dir, "Startup.cs");
                 return File.Exists(moduleConfigFileName);
             }).Order()
            .ToList();

        return moduleDirectories.ConvertAll(Path.GetFileName);
    }
}