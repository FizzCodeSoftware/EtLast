namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using CommandDotNet.Attributes;
    using Serilog.Events;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [ApplicationMetadata(Name = "run", Description = "Execute ETL modules and/or plugins.")]
    [SubCommand]
    public class Run
    {
        [ApplicationMetadata(Name = "module", Description = "Execute one module.")]
        public void RunModule(
            [Argument(Name = "module-name", Description = "The name of the module name.")]string moduleName,
            [Option(LongName = "plugin")]List<string> pluginListOverride,
            [Option(LongName = "param", ShortName = "p")]List<string> moduleSettingOverrides)
        {
            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return;
            }

            CommandLineHandler.Context.Logger.Write(LogEventLevel.Information, "loading module {ModuleName}", moduleName);

            var module = ModuleLoader.LoadModule(CommandLineHandler.Context, moduleName, moduleSettingOverrides?.ToArray(), pluginListOverride?.ToArray());
            if (module?.EnabledPlugins.Count > 0)
            {
                ModuleExecuter.Execute(CommandLineHandler.Context, module);
            }

            ModuleLoader.UnloadModule(CommandLineHandler.Context, module);
        }

        [ApplicationMetadata(Name = "modules", Description = "Execute one or more module.")]
        public void RunModules(
            [Argument(Name = "module-names", Description = "The space-separated list of module names.")]List<string> moduleNames,
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
                CommandLineHandler.Context.Logger.Write(LogEventLevel.Information, "loading module {ModuleName}", moduleName);

                var module = ModuleLoader.LoadModule(CommandLineHandler.Context, moduleName, moduleSettingOverrides?.ToArray(), null);
                if (module == null)
                    return;

                if (module.EnabledPlugins?.Count == 0)
                {
                    CommandLineHandler.Context.Logger.Write(LogEventLevel.Warning, "skipping module {ModuleName} due to it has no enabled plugins", moduleName);
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
                    CommandLineHandler.Context.Logger.Warning("terminating the execution of all modules due to {ModuleName} failed", module.ModuleConfiguration.ModuleName);
                    break;
                }

                Console.WriteLine();
            }
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}