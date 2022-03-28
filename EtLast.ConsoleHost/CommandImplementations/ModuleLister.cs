namespace FizzCode.EtLast.ConsoleHost;

internal static class ModuleLister
{
    public static void ListModules(CommandContext commandContext)
    {
        var moduleNames = GetAllModules(commandContext);
        commandContext.Logger.Information("available modules: {ModuleNames}", moduleNames);
    }

    public static List<string> GetAllModules(CommandContext commandContext)
    {
        var moduleFolders = Directory.GetDirectories(commandContext.HostConfiguration.ModulesFolder)
             .Where(moduleFolder =>
             {
                 var moduleConfigFileName = Path.Combine(moduleFolder, "Startup.cs");
                 return File.Exists(moduleConfigFileName);
             }).OrderBy(x => x)
            .ToList();

        return moduleFolders.ConvertAll(Path.GetFileName);
    }
}
