namespace FizzCode.EtLast.ConsoleHost
{
    using CommandDotNet;

    [Command(Name = "list", Description = "List of modules.")]
    [SubCommand]
    public class ListCommand
    {
        [Command(Name = "modules", Description = "List all available modules.")]
        public void Module()
        {
            ModuleLister.ListModules(CommandLineHandler.Context);
        }
    }
}