using FizzCode.EtLast.HostBuilder;

namespace FizzCode.EtLast;

public class ConsoleHost : AbstractHost
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string HostLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-host");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string DevLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-dev");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string OpsLogFolder { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log-ops");

    public List<string> ReferenceAssemblyFolders { get; } = [];
    public ModuleCompilationMode ModuleCompilationMode { get; internal set; } = ModuleCompilationMode.Dynamic;

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

    public ConsoleHost(string name)
        : base(name)
    {
        ModulesFolder = @".\Modules";
        HostArgumentsFolder = @".\HostArguments";
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\");
        ReferenceAssemblyFolders.Add(@"C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App\");
    }

    protected override ILogger CreateHostLogger()
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

    protected override void ListModules()
    {
        ModuleLister.ListModules(this);
    }

    protected override IArgumentCollection LoadHostArguments()
    {
        return HostArgumentsLoader.LoadHostArguments(this);
    }

    protected override IExecutionResult RunCustomCommand(string[] commandParts)
    {
        switch (commandParts[0].ToLowerInvariant())
        {
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
            case "test-modules":
                var moduleNames = commandParts.Skip(2).ToList();
                if (moduleNames.Count == 0)
                    moduleNames = ModuleLister.GetAllModules(ModulesFolder);

                return TestModules(moduleNames);
        }

        return new ExecutionResult(ExecutionStatusCode.Success);
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

    protected override void ListCommands()
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
}