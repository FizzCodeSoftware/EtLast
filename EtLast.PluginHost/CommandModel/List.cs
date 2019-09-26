namespace FizzCode.EtLast.PluginHost
{
    using CommandDotNet.Attributes;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [ApplicationMetadata(Name = "list", Description = "List modules/plugins.")]
    [SubCommand]
    public class List
    {
        [ApplicationMetadata(Name = "modules", Description = "List all available modules.")]
        public void Module()
        {
            ModuleLister.ListModules(CommandLineHandler.Context);
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}