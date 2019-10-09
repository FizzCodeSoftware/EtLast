namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    internal static class ModuleLister
    {
        public static void ListModules(CommandContext commandContext)
        {
            var moduleNames = GetAllModules(commandContext);
            commandContext.Logger.Information("available modules: {ModuleNames}", moduleNames);
        }

        public static List<string> GetAllModules(CommandContext commandContext)
        {
            var sharedFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, "Shared");

            var moduleFolders = Directory.GetDirectories(commandContext.HostConfiguration.ModulesFolder)
                .Where(x => x != sharedFolder)
                .Where(moduleFolder =>
                {
                    var moduleConfigFileName = Path.Combine(moduleFolder, "module-configuration.json");
                    return File.Exists(moduleConfigFileName);
                })
                .OrderBy(x => x)
                .ToList();

            return moduleFolders.Select(Path.GetFileName).ToList();
        }
    }
}