namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using CommandDotNet;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [Command(Name = "run", Description = "Execute ETL modules and/or plugins.")]
    [SubCommand]
    public class Run
    {
        [Command(Name = "module", Description = "Execute one module.")]
        public void RunModule(
            [Operand(Name = "module-name", Description = "The name of the module.")]string moduleName,
            [Option(LongName = "plugin")]List<string> pluginListOverride,
            [Option(LongName = "param", ShortName = "p")]List<string> moduleSettingOverrides)
        {
            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return;
            }

            CommandLineHandler.Context.Logger.Information("loading module {Module}", moduleName);

            var module = ModuleLoader.LoadModule(CommandLineHandler.Context, moduleName, moduleSettingOverrides?.ToArray(), pluginListOverride?.ToArray());
            if (module?.EnabledPlugins.Count > 0)
            {
                ModuleExecuter.Execute(CommandLineHandler.Context, module);
            }

            if (module != null)
            {
                ModuleLoader.UnloadModule(CommandLineHandler.Context, module);
            }
        }

        [Command(Name = "modules", Description = "Execute one or more module.")]
        public void RunModules(
            [Operand(Name = "module-names", Description = "The space-separated list of module names.")]List<string> moduleNames,
            [Option(LongName = "param", ShortName = "p")]List<string> moduleSettingOverrides)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                CommandLineHandler.DisplayHelp("run modules");
                return;
            }

            var modules = new List<Module>();
            foreach (var moduleName in moduleNames)
            {
                CommandLineHandler.Context.Logger.Information("loading module {Module}", moduleName);

                var module = ModuleLoader.LoadModule(CommandLineHandler.Context, moduleName, moduleSettingOverrides?.ToArray(), null);
                if (module == null)
                    return;

                if (module.EnabledPlugins?.Count == 0)
                {
                    CommandLineHandler.Context.Logger.Warning("skipping module {Module} due to it has no enabled plugins", moduleName);
                    ModuleLoader.UnloadModule(CommandLineHandler.Context, module);

                    continue;
                }

                modules.Add(module);
                Console.WriteLine();
            }

            foreach (var module in modules)
            {
                var result = ModuleExecuter.Execute(CommandLineHandler.Context, module);
                ModuleLoader.UnloadModule(CommandLineHandler.Context, module);

                if (result != ExecutionResult.Success)
                {
                    CommandLineHandler.Context.Logger.Warning("terminating the execution of all modules due to {Module} failed", module.ModuleConfiguration.ModuleName);
                    break;
                }

                Console.WriteLine();
            }
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}