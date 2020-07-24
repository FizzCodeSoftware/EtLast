namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using CommandDotNet;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [Command(Name = "run", Description = "Execute ETL modules and/or plugins.")]
    [SubCommand]
    public class RunCommand
    {
        [Command(Name = "module", Description = "Execute one module.")]
        public void RunModule(
            [Operand(Name = "module-name", Description = "The name of the module.")] string moduleName,
            [Option(LongName = "plugin")] List<string> pluginListOverride,
            [Option(LongName = "param", ShortName = "p")] List<string> moduleSettingOverrides)
        {
            var commandContext = CommandLineHandler.Context;

            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return;
            }

            commandContext.Logger.Information("loading module {Module}", moduleName);

            var module = ModuleLoader.LoadModule(commandContext, moduleName, moduleSettingOverrides?.ToArray(), pluginListOverride?.ToArray(), false);
            if (module == null)
                return;

            if (module.EnabledPlugins.Count > 0)
            {
                ModuleExecuter.Execute(commandContext, module);
            }

            ModuleLoader.UnloadModule(commandContext, module);
        }

        [Command(Name = "modules", Description = "Execute one or more module.")]
        public void RunModules(
            [Operand(Name = "module-names", Description = "The space-separated list of module names.")] List<string> moduleNames,
            [Option(LongName = "param", ShortName = "p")] List<string> moduleSettingOverrides)
        {
            var commandContext = CommandLineHandler.Context;

            if (moduleNames == null || moduleNames.Count == 0)
            {
                CommandLineHandler.DisplayHelp("run modules");
                return;
            }

            var modules = new List<Module>();
            foreach (var moduleName in moduleNames)
            {
                commandContext.Logger.Information("loading module {Module}", moduleName);

                var module = ModuleLoader.LoadModule(commandContext, moduleName, moduleSettingOverrides?.ToArray(), null, false);
                if (module == null)
                    return;

                if (module.EnabledPlugins?.Count == 0)
                {
                    commandContext.Logger.Warning("skipping module {Module} due to it has no enabled plugins", moduleName);
                    ModuleLoader.UnloadModule(commandContext, module);
                    continue;
                }

                modules.Add(module);
            }

            ModuleExecuter.Execute(commandContext, modules.ToArray());

            foreach (var module in modules)
            {
                ModuleLoader.UnloadModule(commandContext, module);
            }
        }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
    }
}