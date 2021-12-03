#pragma warning disable CA1812, CA1822
namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using CommandDotNet;
    using Serilog.Events;

    [Command(">")]
    internal class AppCommands
    {
        [Subcommand]
        public TestCommand Validate { get; set; }

        [Subcommand]
        public ListCommand List { get; set; }

        [Subcommand]
        public RunCommand Run { get; set; }

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
}
#pragma warning restore CA1812, CA1822