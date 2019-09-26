namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using CommandDotNet.Attributes;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [ApplicationMetadata(Name = "validate", Description = "Validates modules/plugins.")]
    [SubCommand]
    public class Validate
    {
        [ApplicationMetadata(Name = "modules", Description = "Validates one or more modules.")]
        public void ValidateModule(
        [Argument(Name = "module-names", Description = "The space-separated list of module names.")]List<string> moduleNames,
        [Option(LongName = "all", ShortName = "a")]bool all)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                if (!all)
                {
                    CommandLineHandler.DisplayHelp("validate modules");
                    return;
                }
            }
            else if (all)
            {
                CommandLineHandler.DisplayHelp("validate modules");
                return;
            }

            var commandContext = CommandLineHandler.Context;

            if (all)
            {
                moduleNames = ModuleLister.GetAllModules(CommandLineHandler.Context);
            }

            foreach (var moduleName in moduleNames)
            {
                var module = ModuleLoader.LoadModule(commandContext, moduleName, null);
                if (module != null)
                {
                    ModuleLoader.UnloadModule(commandContext, module);
                    commandContext.Logger.Information("validation {ValidationResult} for {ModuleName}", "PASSED", moduleName);
                }
                else
                {
                    commandContext.Logger.Information("validation {ValidationResult} for {ModuleName}", "FAILED", moduleName);
                }

                Console.WriteLine();
            }
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}