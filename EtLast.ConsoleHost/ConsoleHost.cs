namespace FizzCode.EtLast;

public class ConsoleHost : IHost
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    public string HostLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-host");
    public string DevLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");
    public string OpsLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");
    public ILogger Logger { get; private set; }

    public string ProgramName { get; }

    public List<string> ReferenceAssemblyFolders { get; } = [];

    public bool SerilogForModulesDisabled { get; set; } = true;
    public bool SerilogForHostEnabled { get; set; } = true;

    public TimeSpan MaxTransactionTimeout { get; set; } = TimeSpan.FromHours(4);

    public ModuleCompilationMode ModuleCompilationMode { get; set; } = ModuleCompilationMode.Dynamic;

    private string _modulesFolder;
    public string ModulesFolder
    {
        get => _modulesFolder;
        set
        {
            _modulesFolder = value;
            if (_modulesFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _modulesFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _modulesFolder[2..]);
            }
        }
    }

    private string _hostArgumentsFolder;
    public string HostArgumentsFolder
    {
        get => _hostArgumentsFolder;
        set
        {
            _hostArgumentsFolder = value;
            if (_hostArgumentsFolder.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _hostArgumentsFolder = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _hostArgumentsFolder[2..]);
            }
        }
    }

    public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    public List<Func<IArgumentCollection, ICommandListener>> CommandListenerCreators { get; } = [];
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListeners { get; } = [];

    private static readonly Regex _regEx = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

    internal ConsoleHost(string programName)
    {
        ProgramName = programName;
        ModulesFolder = @".\Modules";
        HostArgumentsFolder = @".\HostArguments";
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\");
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App\");
    }

    private ILogger CreateHostLogger()
    {
        var config = new LoggerConfiguration();

        if (SerilogForHostEnabled)
        {
            config = config
                .WriteTo.File(Path.Combine(HostLogFolder, "host-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "{Timestamp:HH:mm:ss.fff zzz} [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: int.MaxValue,
                    encoding: Encoding.UTF8)

                .WriteTo.Sink(new ConsoleSink("{Timestamp:HH:mm:ss.fff} [{Level}] {Message} {Properties}{NewLine}{Exception}"), LogEventLevel.Debug);
        }

        config.MinimumLevel.Is(LogEventLevel.Debug);

        return config.CreateLogger();
    }

    public ExecutionStatusCode Run()
    {
        Logger = CreateHostLogger();

        AppDomain.MonitoringIsEnabled = true;
        AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

        Console.WriteLine();
        Logger.Information("{ProgramName} {ProgramVersion} started", ProgramName, Assembly.GetEntryAssembly().GetName().Version.ToString());

        Console.WriteLine();
        Console.WriteLine("Environment:");
        Console.WriteLine("  {0,-23} = {1}", "EtLast", typeof(IEtlContext).Assembly.GetName().Version.ToString());
        Console.WriteLine("  {0,-23} = {1}", "Instance", Environment.MachineName);
        Console.WriteLine("  {0,-23} = {1}", "UserName", Environment.UserName);
        Console.WriteLine("  {0,-23} = {1}", "UserDomainName", Environment.UserDomainName);
        Console.WriteLine("  {0,-23} = {1}", "OSVersion", Environment.OSVersion);
        Console.WriteLine("  {0,-23} = {1}", "ProcessorCount", Environment.ProcessorCount);
        Console.WriteLine("  {0,-23} = {1}", "UserInteractive", Environment.UserInteractive);
        Console.WriteLine("  {0,-23} = {1}", "Is64Bit", Environment.Is64BitProcess);
        Console.WriteLine("  {0,-23} = {1}", "IsPrivileged", Environment.IsPrivilegedProcess);
        Console.WriteLine("  {0,-23} = {1}", "TickCountSinceStartup", Environment.TickCount64);
        Console.WriteLine();

        var commandLineArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
        if (commandLineArgs.Length > 0)
        {
            Logger.Debug("command line arguments: {CommandLineArguments}", commandLineArgs);
            var result = RunCommand(commandLineArgs).Status;

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

        IArgumentCollection hostArguments = null;
        try
        {
            hostArguments = HostArgumentsLoader.LoadHostArguments(this);
            if (hostArguments == null)
            {
                Logger.Write(LogEventLevel.Fatal, "unexpected exception while compiling host arguments");
                return ExecutionStatusCode.HostArgumentError;
            }
        }
        catch (Exception ex)
        {
            Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception while compiling host arguments");
            return ExecutionStatusCode.HostArgumentError;
        }

        SetMaxTransactionTimeout(MaxTransactionTimeout);

        var threads = new List<Thread>();
        foreach (var creator in CommandListenerCreators)
        {
            var listener = creator.Invoke(hostArguments);
            if (listener == null)
                continue;

            var thread = new Thread(() =>
            {
                try
                {
                    listener.Listen(this);
                }
                catch (Exception ex)
                {
                    Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception happened in command line listener");
                }
            });

            threads.Add(thread);
            thread.Start();
        }

        foreach (var thread in threads)
            thread.Join();

        return ExecutionStatusCode.Success;
    }

    public IExecutionResult RunCommand(string command)
    {
        var commandParts = _regEx
            .Matches(command.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommand(commandParts);
    }

    public void Terminate()
    {
        _cancellationTokenSource.Cancel();
    }

    public IExecutionResult RunCommand(string[] commandParts)
    {
        if (commandParts?.Length >= 1 && CommandAliases.TryGetValue(commandParts[0], out var alias))
        {
            commandParts = _regEx
                .Matches(alias.Trim())
                .Select(x => x.Value)
                .Concat(commandParts.Skip(1))
                .ToArray();
        }

        try
        {
            switch (commandParts[0].ToLowerInvariant())
            {
                case "exit":
                    Terminate();
                    break;
                case "help":
                    DisplayHelp();
                    break;
                case "run":
                    {
                        var moduleName = commandParts.Skip(1).FirstOrDefault();
                        if (string.IsNullOrEmpty(moduleName))
                        {
                            Console.WriteLine("Missing module name. Usage: `run <moduleName> <taskNames>`");
                            return new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
                        }

                        var taskNames = commandParts.Skip(2).ToList();
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
                    var moduleNames = commandParts.Skip(2).ToList();
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

            ModuleLoader.LoadModule(this, moduleName, ModuleCompilationMode.ForceCompilation, out var module);
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

        var loadResult = ModuleLoader.LoadModule(this, moduleName, ModuleCompilationMode, out var module);
        if (loadResult != ExecutionStatusCode.Success)
            return new ExecutionResult(loadResult);

        foreach (var taskName in taskNames)
        {
            var taskType = module.TaskTypes.Find(x => string.Equals(x.Name, taskName, StringComparison.InvariantCultureIgnoreCase));
            if (taskType == null)
            {
                Logger.Warning("unknown task type: " + taskName);
                break;
            }
        }

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

    public List<string> GetReferenceAssemblyFileNames()
    {
        var referenceDllFileNames = new List<string>();
        foreach (var referenceAssemblyFolder in ReferenceAssemblyFolders)
        {
#if NET8_0
            var folder = Directory.GetDirectories(referenceAssemblyFolder, "8.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();
#endif
#if NET7_0
            var folder = Directory.GetDirectories(referenceAssemblyFolder, "7.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();
#endif
#if NET6_0
            var folder = Directory.GetDirectories(referenceAssemblyFolder, "6.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();
#endif

            Logger.Information("using assemblies from {ReferenceAssemblyFolder}", folder);

            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "System*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.AspNetCore*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.Extensions*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.Net*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "netstandard.dll", SearchOption.TopDirectoryOnly));
        }

        var referenceFileNames = referenceDllFileNames
            .Where(x => !Path.GetFileNameWithoutExtension(x).EndsWith("Native", StringComparison.InvariantCultureIgnoreCase))
            .ToList();

        var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly)
            .Where(x => Path.GetFileName(x) != "FizzCode.EtLast.ConsoleHost.dll"
                && !Path.GetFileName(x).Equals("testhost.dll", StringComparison.InvariantCultureIgnoreCase));

        referenceFileNames.AddRange(localDllFileNames);

        return referenceFileNames
            .Distinct()
            .Where(x =>
            {
                try
                {
                    var _ = AssemblyName.GetAssemblyName(x);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            })
            .ToList();
    }

    private void SetMaxTransactionTimeout(TimeSpan maxValue)
    {
        if (TransactionManager.MaximumTimeout == maxValue)
            return;

        Logger.Write(LogEventLevel.Information, "maximum transaction timeout is set to {MaxTransactionTimeout}", maxValue);

        var field = typeof(TransactionManager).GetField("s_cachedMaxTimeout", BindingFlags.NonPublic | BindingFlags.Static);
        field.SetValue(null, true);

        field = typeof(TransactionManager).GetField("s_maximumTimeout", BindingFlags.NonPublic | BindingFlags.Static);
        field.SetValue(null, maxValue);
    }
}