namespace FizzCode.EtLast.ConsoleHost;

public class Host : IHost
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    public string HostLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-host");
    public string DevLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");
    public string OpsLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");
    public ILogger Logger { get; private set; }

    public string ProgramName { get; }

    public List<string> ReferenceAssemblyFolders { get; } = new List<string>();

    public bool SerilogForModulesEnabled { get; set; } = true;
    public bool SerilogForCommandsEnabled { get; set; } = true;

    private string _modulesFolder;
    public string ModulesFolder
    {
        get => _modulesFolder; set
        {
            _modulesFolder = value;
            if (_modulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _modulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _modulesFolder[2..]);
            }
        }
    }

    public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    public List<ICommandLineListener> CommandLineListeners { get; } = new List<ICommandLineListener>();
    public List<Func<IEtlSession, IEtlContextListener>> EtlContextListeners { get; } = new List<Func<IEtlSession, IEtlContextListener>>();
    public string[] CommandLineArgs { get; set; }

    private static readonly Regex _regEx = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

    internal Host(string programName)
    {
        ProgramName = programName;
        ModulesFolder = @".\modules";
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\");
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App\");
    }

    private ILogger CreateLogger()
    {
        var config = new LoggerConfiguration();

        if (SerilogForCommandsEnabled)
        {
            config = config
                .WriteTo.File(Path.Combine(HostLogFolder, "commands-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    retainedFileCountLimit: int.MaxValue,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    rollingInterval: RollingInterval.Day,
                    formatProvider: CultureInfo.InvariantCulture,
                    encoding: Encoding.UTF8)

                .WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"), LogEventLevel.Debug);
        }

        config.MinimumLevel.Is(LogEventLevel.Debug);

        return config.CreateLogger();
    }

    public ExecutionStatusCode Run()
    {
        Logger = CreateLogger();

        AppDomain.MonitoringIsEnabled = true;
        AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

        Console.WriteLine();
        Logger.Information("{ProgramName} {ProgramVersion} started on {EtLast} {EtLastVersion}", ProgramName, Assembly.GetEntryAssembly().GetName().Version.ToString(), "EtLast", typeof(IEtlContext).Assembly.GetName().Version.ToString());

        if (CommandLineArgs?.Length > 0)
        {
            Logger.Debug("command line arguments: {CommandLineArguments}", CommandLineArgs);
            var result = RunCommandLine(CommandLineArgs).Status;

            if (Debugger.IsAttached)
            {
                Console.WriteLine();
                Console.WriteLine("done, press any key to continue");
                Console.ReadKey();
            }

            return result;
        }

        DisplayHelp();
        ModuleLister.ListModules(this);

        var threads = new List<Thread>();
        foreach (var listener in CommandLineListeners)
        {
            var thread = new Thread(() =>
            {
                listener.Listen(this);
            });

            threads.Add(thread);
            thread.Start();
        }

        foreach (var thread in threads)
            thread.Join();

        return ExecutionStatusCode.Success;
    }

    public Host UseCommandLineListener(ICommandLineListener listener)
    {
        CommandLineListeners.Add(listener);
        return this;
    }

    public IExecutionResult RunCommandLine(string commandLine)
    {
        var commandLineParts = _regEx
            .Matches(commandLine.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommandLine(commandLineParts);
    }

    public IExecutionResult RunCommandLine(string[] commandLineParts)
    {
        if (commandLineParts?.Length >= 1 && CommandAliases.TryGetValue(commandLineParts[0], out var alias))
        {
            commandLineParts = _regEx
                .Matches(alias.Trim())
                .Select(x => x.Value)
                .Concat(commandLineParts.Skip(1))
                .ToArray();
        }

        try
        {
            switch (commandLineParts[0].ToLowerInvariant())
            {
                case "exit":
                    _cancellationTokenSource.Cancel();
                    break;
                case "help":
                    DisplayHelp();
                    break;
                case "run":
                    {
                        var moduleName = commandLineParts.Skip(1).FirstOrDefault();
                        if (string.IsNullOrEmpty(moduleName))
                        {
                            Console.WriteLine("Missing module name. Usage: `run <moduleName> <taskNames>`");
                            return new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
                        }

                        var taskNames = commandLineParts.Skip(2).ToList();
                        if (taskNames.Count == 0)
                        {
                            Console.WriteLine("Missing task name(s). Usage: `run <moduleName> <taskNames>`");
                            return new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
                        }

                        return RunModule(moduleName, taskNames);
                    }
                case "list-modules":
                    ModuleLister.ListModules(this);
                    return new ExecutionResult(ExecutionStatusCode.Success);
                case "test-modules":
                    var moduleNames = commandLineParts.Skip(2).ToList();
                    if (moduleNames.Count == 0)
                        moduleNames = ModuleLister.GetAllModules(ModulesFolder);

                    return TestModules(moduleNames);
            }

            return new ExecutionResult(ExecutionStatusCode.Success);
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            Logger.Write(LogEventLevel.Fatal, "unexpected error during execution: {ErrorMessage}", formattedMessage);

            return new ExecutionResult(ExecutionStatusCode.UnexpectedError);
        }
    }

    private IExecutionResult TestModules(List<string> moduleNames)
    {
        var result = new ExecutionResult();
        foreach (var moduleName in moduleNames)
        {
            Logger.Information("loading module {Module}", moduleName);

            ModuleLoader.LoadModule(this, moduleName, true, out var module);
            if (module != null)
            {
                ModuleLoader.UnloadModule(this, module);
                Logger.Information("validation {ValidationResult} for {Module}", "PASSED", moduleName);
            }
            else
            {
                Logger.Information("validation {ValidationResult} for {Module}", "FAILED", moduleName);
                result.Status = ExecutionStatusCode.ModuleLoadError;
            }
        }

        return result;
    }

    private IExecutionResult RunModule(string moduleName, List<string> taskNames)
    {
        Logger.Information("loading module {Module}", moduleName);

        var loadResult = ModuleLoader.LoadModule(this, moduleName, false, out var module);
        if (loadResult != ExecutionStatusCode.Success)
            return new ExecutionResult(loadResult);

        var executionResult = ModuleExecuter.Execute(this, module, taskNames.ToArray());

        ModuleLoader.UnloadModule(this, module);
        return executionResult;
    }

    private void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
    {
        if (e?.ExceptionObject is not Exception ex)
            return;

        var formattedMessage = ex.FormatExceptionWithDetails();

        if (Logger != null)
        {
            Logger.Write(LogEventLevel.Fatal, "unexpected error during execution: {ErrorMessage}", formattedMessage);
        }
        else
        {
            Console.WriteLine("unexpected error during execution: " + formattedMessage);
        }

        Environment.Exit(-1);
    }

    internal void DisplayHelp()
    {
        Console.WriteLine("Commands:");
        Console.WriteLine("  run          <moduleName> <taskNames>");
        Console.WriteLine("  list-modules");
        Console.WriteLine("  test-modules [moduleNames]");
        Console.WriteLine("  exit");

        Console.WriteLine();

        if (CommandAliases?.Count > 0)
        {
            Console.WriteLine("Aliases:");
            var maxAliasLength = CommandAliases.Max(x => x.Key.Length);
            foreach (var alias in CommandAliases)
            {
                Console.WriteLine("  " + alias.Key.PadRight(maxAliasLength, ' ') + "  '" + alias.Value + "'");
            }

            Console.WriteLine();
        }
    }
}