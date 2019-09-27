namespace FizzCode.EtLast.PluginHost
{
    using CommandDotNet.Attributes;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
    [ApplicationMetadata(Name = ">")]
    internal class AppCommands
    {
        [SubCommand]
        public Test Validate { get; set; }

        [SubCommand]
        public List List { get; set; }

        [SubCommand]
        public Execute Execute { get; set; }

        [ApplicationMetadata(Name = "exit", Description = "Exit from the command-line utility.")]
        public void Exit()
        {
            CommandLineHandler.Terminated = true;
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
}