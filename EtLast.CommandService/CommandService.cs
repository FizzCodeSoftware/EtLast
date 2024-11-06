namespace FizzCode.EtLast;

public partial class CommandService : AbstractCommandService
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string ServiceLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log", "svc");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string DevLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log", "dev");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string OpsLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log", "ops");

    public List<string> ReferenceAssemblyDirectories { get; } = [];

    private string _modulesDirectory;
    public string ModulesDirectory
    {
        get => _modulesDirectory;
        set
        {
            _modulesDirectory = value;
            if (_modulesDirectory.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _modulesDirectory = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _modulesDirectory[2..]);
            }
        }
    }

    private string _serviceArgumentsDirectory;
    public string ServiceArgumentsDirectory
    {
        get => _serviceArgumentsDirectory;
        set
        {
            _serviceArgumentsDirectory = value;
            if (_serviceArgumentsDirectory.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _serviceArgumentsDirectory = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _serviceArgumentsDirectory[2..]);
            }
        }
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<CommandServiceFluent.SessionBuilderAction> SessionConfigurators { get; } = [];

    public CommandService(string name)
        : base(name)
    {
        ModulesDirectory = @".\Modules";
        ServiceArgumentsDirectory = @".\ServiceArguments";
        ReferenceAssemblyDirectories.Add(@"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\");
        ReferenceAssemblyDirectories.Add(@"C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App\");

        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
    }

    protected override IExecutionResult RunCustomCommand(string commandId, string originalCommand, string[] commandParts)
    {
        return new ExecutionResult(ExecutionStatusCode.Success);
    }

    protected override ILogger CreateServiceLogger()
    {
        var config = new LoggerConfiguration();

        if (ServiceLoggingEnabled)
        {
            config = config
                .WriteTo.File(Path.Combine(ServiceLogDirectory, "svc-.txt"),
                    restrictedToMinimumLevel: LogEventLevel.Debug,
                    outputTemplate: "[{Timestamp:HH:mm:ss.fff zzz}] [{ServiceId}] [{Level:u3}] {Message:l} {NewLine}{Exception}",
                    formatProvider: CultureInfo.InvariantCulture,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: int.MaxValue,
                    encoding: Encoding.UTF8)

                .WriteTo.Sink(new ConsoleSink("[{Timestamp:HH:mm:ss.fff}] [{ServiceId}] {Message} {Properties}{NewLine}{Exception}"),
                    LogEventLevel.Debug);
        }

        config.MinimumLevel.Is(LogEventLevel.Debug);
        config.Enrich.WithProperty("ServiceId", Environment.ProcessId);

        return config.CreateLogger();
    }

    protected override void ListModules()
    {
        ModuleLister.ListModules(this);
    }

    protected override ArgumentCollection LoadServiceArguments()
    {
        return ServiceArgumentsLoader.LoadServiceArguments(this);
    }

    protected override IExecutionResult TestModulesInternal(List<string> moduleNames)
    {
        if (moduleNames.Count == 0)
            moduleNames = ModuleLister.GetAllModules(ModulesDirectory);

        var result = new ExecutionResult();
        foreach (var moduleName in moduleNames)
        {
            Logger.Information("loading module {Module}", moduleName);

            ModuleLoader.LoadModule(this, moduleName, false, true, out var module);
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

    protected override IExecutionResult RunTasksInModuleByNameInternal(bool useAppDomain, string commandId, string moduleName, List<string> taskNames, Dictionary<string, string> userArguments, Dictionary<string, object> argumentOverrides)
    {
        Logger.Information("loading module {Module}", moduleName);

        var loadResult = ModuleLoader.LoadModule(this, moduleName, useAppDomain, discoverTasks: true, out var module);
        if (loadResult != ExecutionStatusCode.Success)
            return new ExecutionResult(loadResult);

        var tasks = new List<IEtlTask>();
        foreach (var taskName in taskNames)
        {
            var taskType = module.TaskTypes.Find(x => string.Equals(x.Name, taskName, StringComparison.InvariantCultureIgnoreCase))
                ?? module.PreCompiledTaskTypes?.Find(x => string.Equals(x.FullName, taskName, StringComparison.InvariantCultureIgnoreCase));

            if (taskType != null)
            {
                tasks.Add((IEtlTask)Activator.CreateInstance(taskType));
            }
            else
            {
                Logger.Error("unknown task type: " + taskName);
                return new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
            }
        }

        try
        {
            var arguments = new ArgumentCollection(ServiceArguments, module.DefaultArgumentProviders, module.InstanceArgumentProviders, userArguments, argumentOverrides);
            var result = RunTasks(commandId, module.Name, module.Startup, tasks, arguments);
            return result;
        }
        finally
        {
            ModuleLoader.UnloadModule(this, module);
        }
    }

    protected override IExecutionResult RunTasksInModuleInternal(bool useAppDomain, string commandId, string originalCommand, string moduleName, List<IEtlTask> tasks, Dictionary<string, string> userArguments, Dictionary<string, object> argumentOverrides)
    {
        Logger.Information("loading module {Module}", moduleName);

        var loadResult = ModuleLoader.LoadModule(this, moduleName, useAppDomain, discoverTasks: false, out var module);
        if (loadResult != ExecutionStatusCode.Success)
            return new ExecutionResult(loadResult);

        try
        {
            var arguments = new ArgumentCollection(ServiceArguments, module.DefaultArgumentProviders, module.InstanceArgumentProviders, userArguments, argumentOverrides);
            var result = RunTasks(commandId, module.Name, module.Startup, tasks, arguments);
            return result;
        }
        finally
        {
            ModuleLoader.UnloadModule(this, module);
        }
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

    protected override void ListCommands()
    {
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  run          <moduleName> <taskNames> [property=value]");
        Console.WriteLine("  rundomain    <moduleName> <taskNames> [property=value]");
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

    public List<string> GetReferenceAssemblyFilePaths()
    {
        var referenceDllFilePaths = new List<string>();
        foreach (var referenceAssemblyDirectory in ReferenceAssemblyDirectories)
        {
            var directory = Directory.GetDirectories(referenceAssemblyDirectory, "9.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();

            Logger.Information("using assemblies from {ReferenceAssemblyDirectory}", directory);

            referenceDllFilePaths.AddRange(Directory.GetFiles(directory, "System*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFilePaths.AddRange(Directory.GetFiles(directory, "Microsoft.AspNetCore*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFilePaths.AddRange(Directory.GetFiles(directory, "Microsoft.Extensions*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFilePaths.AddRange(Directory.GetFiles(directory, "Microsoft.Net*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFilePaths.AddRange(Directory.GetFiles(directory, "netstandard.dll", SearchOption.TopDirectoryOnly));
        }

        var referenceFilePaths = referenceDllFilePaths
            .Where(x => !Path.GetFileNameWithoutExtension(x).EndsWith("Native", StringComparison.InvariantCultureIgnoreCase))
            .ToList();

        var selfDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        var localDllFilePaths = Directory.GetFiles(selfDirectory, "*.dll", SearchOption.TopDirectoryOnly)
            .Where(x => !Path.GetFileName(x).StartsWith("FizzCode.EtLast.CommandService.", StringComparison.InvariantCultureIgnoreCase)
                && !Path.GetFileName(x).Equals("testhost.dll", StringComparison.InvariantCultureIgnoreCase));

        referenceFilePaths.AddRange(localDllFilePaths);

        return referenceFilePaths
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
}