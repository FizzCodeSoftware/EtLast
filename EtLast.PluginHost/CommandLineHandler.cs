namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Linq;
    using CommandDotNet;
    using CommandDotNet.Models;

    public static class CommandLineHandler
    {
        public static string[] StartupArguments { get; private set; }
        public static bool Terminated { get; set; }
        public static CommandContext Context { get; private set; }
        private static AppRunner<AppCommands> _runner;

        public static void Run(string[] startupArguments)
        {
            Context = new CommandContext();
            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

            _runner = new AppRunner<AppCommands>(GetAppSettings());

            try
            {
                Context.Load();
            }
            catch
            {
                return;
            }

            StartupArguments = startupArguments;

            if (StartupArguments?.Length > 0)
            {
                RunCommand(StartupArguments);
                return;
            }

            DisplayHelp();

            DisplayModuleNames();

            while (!Terminated)
            {
                Console.Write("> ");
                var commandLine = Console.ReadLine();
                if (string.IsNullOrEmpty(commandLine))
                    continue;

                var lineArguments = commandLine.Split(' ');
                Run(lineArguments);

                Console.WriteLine();
            }
        }

        public static void RunCommand(params string[] args)
        {
            _runner.Run(args);
        }

        private static void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            if (Context.Logger != null)
            {
                Context.Logger.Error(e.ExceptionObject as Exception, "unexpected error during execution");
                Context.OpsLogger.Error("unexpected error during execution: {Message}", (e.ExceptionObject as Exception)?.Message);
            }
            else
            {
                Console.WriteLine("unexpected error during execution: " + e.ExceptionObject.ToString());
            }

            Environment.Exit(-1);
        }

        internal static void DisplayHelp(string command = null)
        {
            if (string.IsNullOrEmpty(command))
            {
                RunCommand("--help");
            }
            else
            {
                var args = command.Split(' ').ToList();
                args.Add("--help");
                RunCommand(args.ToArray());
            }
        }

        private static void DisplayModuleNames()
        {
            ModuleLister.ListModules(Context);
        }

        private static AppSettings GetAppSettings()
        {
            return new AppSettings()
            {
                EnableVersionOption = false,
                Case = Case.KebabCase,
                Help = new AppHelpSettings()
                {
                    TextStyle = HelpTextStyle.Basic,
                    UsageAppNameStyle = UsageAppNameStyle.GlobalTool,
                },
            };
        }
    }
}