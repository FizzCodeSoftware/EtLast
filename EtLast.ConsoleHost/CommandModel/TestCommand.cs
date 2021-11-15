namespace FizzCode.EtLast.ConsoleHost
{
    using System.Collections.Generic;
    using CommandDotNet;

    [Command(Name = "test", Description = "Test connection strings, modules, etc.")]
    [SubCommand]
    public class TestCommand
    {
        [Command(Name = "modules", Description = "Tests one or more modules.")]
        public int ValidateModule(
        [Operand(Name = "names", Description = "The space-separated list of module names.")] List<string> moduleNames,
        [Option(LongName = "all", ShortName = "a")] bool all)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                if (!all)
                {
                    CommandLineHandler.DisplayHelp("test modules");
                    return (int)ExecutionResult.HostArgumentError;
                }
            }
            else if (all)
            {
                CommandLineHandler.DisplayHelp("test modules");
                return (int)ExecutionResult.HostArgumentError;
            }

            var commandContext = CommandLineHandler.Context;

            if (all)
            {
                moduleNames = ModuleLister.GetAllModules(commandContext);
            }

            var result = ExecutionResult.Success;

            foreach (var moduleName in moduleNames)
            {
                commandContext.Logger.Information("loading module {Module}", moduleName);

                ModuleLoader.LoadModule(commandContext, moduleName, true, out var module);
                if (module != null)
                {
                    ModuleLoader.UnloadModule(commandContext, module);
                    commandContext.Logger.Information("validation {ValidationResult} for {Module}", "PASSED", moduleName);
                }
                else
                {
                    commandContext.Logger.Information("validation {ValidationResult} for {Module}", "FAILED", moduleName);
                    result = ExecutionResult.ModuleLoadError;
                }
            }

            return (int)result;
        }
    }
}