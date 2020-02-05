namespace FizzCode.EtLast.PluginHost
{
    using CommandDotNet;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
    [Command(Name = ">")]
    internal class AppCommands
    {
        [SubCommand]
        public Test Validate { get; set; }

        [SubCommand]
        public List List { get; set; }

        [SubCommand]
        public Run Run { get; set; }

        [Command(Name = "exit", Description = "Exit from the command-line utility.")]
        public void Exit()
        {
            CommandLineHandler.Terminated = true;
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
}