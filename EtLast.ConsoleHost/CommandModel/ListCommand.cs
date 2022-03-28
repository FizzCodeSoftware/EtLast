namespace FizzCode.EtLast.ConsoleHost;

[Command("list", Description = "List of modules.")]
[Subcommand]
public class ListCommand
{
    [Command("modules", Description = "List all available modules.")]
    public void Module()
    {
        ModuleLister.ListModules(CommandLineHandler.Context);
    }
}
