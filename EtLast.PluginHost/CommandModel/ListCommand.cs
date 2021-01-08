namespace FizzCode.EtLast.PluginHost
{
    using CommandDotNet;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
    [Command(Name = "list", Description = "List of modules and plugins.")]
    [SubCommand]
    public class ListCommand
    {
        [Command(Name = "modules", Description = "List all available modules.")]
        public void Module()
        {
            ModuleLister.ListModules(CommandLineHandler.Context);
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
}