namespace FizzCode.EtLast.ConsoleHost
{
    using System.Collections.Generic;
    using CommandDotNet;

    [Command("run", Description = "Execute ETL commands.")]
    [Subcommand]
    public class RunCommand
    {
        [Command("command", Description = "Execute one command.")]
        public int RunModule(
            [Operand("module", Description = "The name of the module.")] string moduleName,
            [Operand("command", Description = "The command.")] string command)
        {
            var commandContext = CommandLineHandler.Context;

            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return (int)ExecutionResult.HostArgumentError;
            }

            commandContext.Logger.Information("loading module {Module}", moduleName);

            var loadResult = ModuleLoader.LoadModule(commandContext, moduleName, false, out var module);
            if (loadResult != ExecutionResult.Success)
                return (int)loadResult;

            var executionResult = ModuleExecuter.Execute(commandContext, module, new[] { command });

            ModuleLoader.UnloadModule(commandContext, module);

            return (int)executionResult;
        }

        [Command("commands", Description = "Execute one or more commands.")]
        public int RunModules(
            [Operand("module", Description = "The name of the module.")] string moduleName,
            [Operand("commands", Description = "The space-separated list of task names.")] List<string> commands)
        {
            var commandContext = CommandLineHandler.Context;

            if (string.IsNullOrEmpty(moduleName))
            {
                CommandLineHandler.DisplayHelp("run module");
                return (int)ExecutionResult.HostArgumentError;
            }

            commandContext.Logger.Information("loading module {Module}", moduleName);

            var loadResult = ModuleLoader.LoadModule(commandContext, moduleName, false, out var module);
            if (loadResult != ExecutionResult.Success)
                return (int)loadResult;

            var executionResult = ExecutionResult.Success;
            if (commands.Count > 0)
            {
                executionResult = ModuleExecuter.Execute(commandContext, module, commands.ToArray());
            }

            ModuleLoader.UnloadModule(commandContext, module);

            return (int)executionResult;
        }
    }
}