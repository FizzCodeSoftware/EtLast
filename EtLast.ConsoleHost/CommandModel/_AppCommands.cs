#pragma warning disable CA1812, CA1822
namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using CommandDotNet;
    using Serilog.Events;

    [Command(Name = ">")]
    internal class AppCommands
    {
        [SubCommand]
        public TestCommand Validate { get; set; }

        [SubCommand]
        public ListCommand List { get; set; }

        [SubCommand]
        public RunCommand Run { get; set; }

        [Command(Name = "protect", Description = "Protect a secret.")]
        public void ProtectSecret([Operand(Name = "secret", Description = "The secret to protect.")] string secret)
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

        [Command(Name = "exit", Description = "Exit from the command-line utility.")]
        public void Exit()
        {
            CommandLineHandler.Terminated = true;
        }
    }
}
#pragma warning restore CA1812, CA1822