namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using System.Threading;
    using CommandDotNet;
    using CommandDotNet.Help;

    public static class CommandLineHandler
    {
        public static string[] StartupArguments { get; private set; }
        public static bool Terminated { get; set; }
        public static CommandContext Context { get; private set; }

        private static AppRunner<AppCommands> _runner;
        private static readonly Regex _regEx = new Regex("(?<=\")[^\"]*(?=\")|[^\" ]+");

        public static void Run(string programName, string[] startupArguments)
        {
            Context = new CommandContext();
            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

            _runner = new AppRunner<AppCommands>(GetAppSettings());

            try
            {
                if (!Context.Load())
                    return;
            }
            catch (Exception ex)
            {
                var msg = ex.FormatExceptionWithDetails(false);
                Console.WriteLine("error during initialization:");
                Console.WriteLine(msg);
                if (Debugger.IsAttached)
                {
                    Console.WriteLine("press any key to exit...");
                    Console.ReadKey();
                }
                else
                {
                    Thread.Sleep(3000);
                }

                return;
            }

            Console.WriteLine();
            Context.Logger.Information("{ProgramName} {ProgramVersion} started on {EtLast} {EtLastVersion}", programName, Assembly.GetEntryAssembly().GetName().Version.ToString(), "EtLast", typeof(IEtlContext).Assembly.GetName().Version.ToString());
            Context.Logger.Debug("command line arguments: {CommandLineArguments}", startupArguments);
            if (!string.IsNullOrEmpty(Context.HostConfiguration.SeqUrl))
            {
                Context.Logger.Debug("all logs will be sent to SEQ listening on {SeqUrl}", Context.HostConfiguration.SeqUrl);
            }

            Console.WriteLine();

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

                var lineArguments = _regEx
                    .Matches(commandLine.Trim())
                    .Select(x => x.Value)
                    .ToArray();

                RunCommand(lineArguments);

                Console.WriteLine();
            }
        }

        public static void RunCommand(params string[] args)
        {
            if (args?.Length >= 1)
            {
                foreach (var kvp in Context.HostConfiguration.CommandAliases)
                {
                    if (string.Equals(kvp.Key, args[0], StringComparison.InvariantCultureIgnoreCase))
                    {
                        args = _regEx
                            .Matches(kvp.Value.Trim())
                            .Select(x => x.Value)
                            .Concat(args.Skip(1))
                            .ToArray();
                        break;
                    }
                }
            }

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
                Help = new AppHelpSettings()
                {
                    TextStyle = HelpTextStyle.Basic,
                    UsageAppName = ">",
                    PrintHelpOption = false,
                },
            };
        }
    }
}