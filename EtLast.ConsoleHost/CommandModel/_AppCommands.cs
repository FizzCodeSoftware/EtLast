namespace FizzCode.EtLast.ConsoleHost;

[Command(">")]
internal class AppCommands
{
    [Subcommand]
    public TestCommand Validate { get; set; }

    [Subcommand]
    public ListCommand List { get; set; }

    [Command("run", Description = "Execute one or more commands.")]
    public int Run(
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

    [Command("protect", Description = "Protect a secret.")]
    public void ProtectSecret([Operand("secret", Description = "The secret to protect.")] string secret)
    {
        var commandContext = CommandLineHandler.Context;

        if (commandContext.HostConfiguration.SecretProtector == null)
        {
            commandContext.Logger.Write(LogEventLevel.Fatal, "secret protector is not set in {HostConfigurationFileName}", PathHelpers.GetFriendlyPathName(commandContext.LoadedConfigurationFileName));
            return;
        }

        if (string.IsNullOrEmpty(secret))
        {
            CommandLineHandler.DisplayHelp("protect");
            return;
        }

        var protectedSecret = commandContext.HostConfiguration.SecretProtector.Encrypt(secret);
        Console.WriteLine("The protected secret is:");
        Console.WriteLine("-------------");
        Console.WriteLine(protectedSecret);
        Console.WriteLine("-------------");
    }

    [Command("exit", Description = "Exit from the command-line utility.")]
    public void Exit()
    {
        CommandLineHandler.Terminated = true;
    }
}