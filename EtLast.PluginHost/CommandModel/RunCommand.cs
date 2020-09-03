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
        public int RunModule(
            [Operand(Name = "module-name", Description = "The name of the module.")] string moduleName,
            [Option(LongName = "plugin")] List<string> pluginListOverride,
            [Option(LongName = "param", ShortName = "p")] List<string> moduleSettingOverrides)
        {
            var commandContext = CommandLineHandler.Context;

            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return (int)ExecutionResult.HostArgumentError;
            }

            commandContext.Logger.Information("loading module {Module}", moduleName);

            var loadResult = ModuleLoader.LoadModule(commandContext, moduleName, moduleSettingOverrides?.ToArray(), pluginListOverride?.ToArray(), false, out var module);
            if (loadResult != ExecutionResult.Success)
                return (int)loadResult;

            var executionResult = ExecutionResult.Success;
            if (module.EnabledPlugins.Count > 0)
            {
                executionResult = ModuleExecuter.Execute(commandContext, module);
            }

            ModuleLoader.UnloadModule(commandContext, module);

            return (int)executionResult;
        }

        [Command(Name = "modules", Description = "Execute one or more module.")]
        public int RunModules(
            [Operand(Name = "module-names", Description = "The space-separated list of module names.")] List<string> moduleNames,
            [Option(LongName = "param", ShortName = "p")] List<string> moduleSettingOverrides)
        {
            var commandContext = CommandLineHandler.Context;

            if (moduleNames == null || moduleNames.Count == 0)
            {
                CommandLineHandler.DisplayHelp("run modules");
                return (int)ExecutionResult.HostArgumentError;
            }

            var modules = new List<Module>();
            foreach (var moduleName in moduleNames)
            {
                commandContext.Logger.Information("loading module {Module}", moduleName);

                var loadResult = ModuleLoader.LoadModule(commandContext, moduleName, moduleSettingOverrides?.ToArray(), null, false, out var module);
                if (loadResult != ExecutionResult.Success)
                    return (int)loadResult;

                if (module.EnabledPlugins?.Count == 0)
                {
                    commandContext.Logger.Warning("skipping module {Module} due to it has no enabled plugins", moduleName);
                    ModuleLoader.UnloadModule(commandContext, module);
                    continue;
                }

                modules.Add(module);
            }

            var executionResult = ModuleExecuter.Execute(commandContext, modules.ToArray());

            foreach (var module in modules)
            {
                ModuleLoader.UnloadModule(commandContext, module);
            }

            return (int)executionResult;
        }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
    }
}