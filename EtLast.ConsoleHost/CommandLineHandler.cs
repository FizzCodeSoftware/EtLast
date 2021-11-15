namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using System.Threading;
    using CommandDotNet;
    using CommandDotNet.Help;
    using Serilog.Events;

    public static class CommandLineHandler
    {
        public static bool Terminated { get; set; }
        public static CommandContext Context { get; private set; }

        private static AppRunner<AppCommands> _runner;
        private static readonly Regex _regEx = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

        public static ExecutionResult Run(string programName, string[] startupArguments)
        {
            Context = new CommandContext();
            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

            _runner = new AppRunner<AppCommands>(GetAppSettings());

            try
            {
                if (!Context.Load())
                    return ExecutionResult.HostConfigurationError;
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

                return ExecutionResult.HostConfigurationError;
            }

            Console.WriteLine();
            Context.Logger.Information("{ProgramName} {ProgramVersion} started on {EtLast} {EtLastVersion}", programName, Assembly.GetEntryAssembly().GetName().Version.ToString(), "EtLast", typeof(IEtlContext).Assembly.GetName().Version.ToString());

            if (startupArguments?.Length > 0)
            {
                Context.Logger.Debug("command line arguments: {CommandLineArguments}", startupArguments);
            }

            Console.WriteLine();

            if (startupArguments?.Length > 0)
            {
                var exitCode = RunCommand(startupArguments);
                return exitCode;
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

            return ExecutionResult.Success;
        }

        public static ExecutionResult RunCommand(params string[] args)
        {
            if (args?.Length >= 1 && Context.HostConfiguration.CommandAliases.TryGetValue(args[0], out var alias))
            {
                args = _regEx
                    .Matches(alias.Trim())
                    .Select(x => x.Value)
                    .Concat(args.Skip(1))
                    .ToArray();
            }

            try
            {
                var exitCode = _runner.Run(args);
                return (ExecutionResult)exitCode;
            }
            catch (Exception ex)
            {
                var formattedMessage = ex.FormatExceptionWithDetails();
                Context.Logger.Write(LogEventLevel.Fatal, "unexpected error during execution: {ErrorMessage}", formattedMessage);

                return ExecutionResult.UnexpectedError;
            }
        }

        private static void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            if (e?.ExceptionObject is not Exception ex)
                return;

            var formattedMessage = ex.FormatExceptionWithDetails();

            if (Context.Logger != null)
            {
                Context.Logger.Write(LogEventLevel.Fatal, "unexpected error during execution: {ErrorMessage}", formattedMessage);
            }
            else
            {
                Console.WriteLine("unexpected error during execution: " + formattedMessage);
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

            Console.WriteLine();

            if (Context.HostConfiguration.CommandAliases?.Count > 0)
            {
                Console.WriteLine("Aliases:");
                var maxAliasLength = Context.HostConfiguration.CommandAliases.Max(x => x.Key.Length);
                foreach (var alias in Context.HostConfiguration.CommandAliases)
                {
                    Console.WriteLine("  " + alias.Key.PadRight(maxAliasLength, ' ') + "  '" + alias.Value + "'");
                }

                Console.WriteLine();
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