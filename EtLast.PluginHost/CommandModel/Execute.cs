namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using CommandDotNet.Attributes;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [ApplicationMetadata(Name = "execute", Description = "Execute modules/plugins.")]
    [SubCommand]
    public class Execute
    {
        [ApplicationMetadata(Name = "modules", Description = "Execute one or more modules.")]
        public void ExecuteModule(
            [Argument(Name = "module-names", Description = "The space-separated list of module names.")]List<string> moduleNames,
            [Option(LongName = "param", ShortName = "p")]List<string> moduleSettingOverrides)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                CommandLineHandler.DisplayHelp("execute module");
                return;
            }

            foreach (var moduleName in moduleNames)
            {
                var result = ModuleExecuter.Execute(CommandLineHandler.Context, moduleName, moduleSettingOverrides?.ToArray());
                if (result != ExecutionResult.Success)
                {
                    CommandLineHandler.Context.Logger.Warning("terminating the execution of all modules due to {ModuleName} failed", moduleName);
                    break;
                }

                Console.WriteLine();
            }
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}