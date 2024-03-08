using Microsoft.Extensions.Hosting;

namespace FizzCode.EtLast;

public class ConsoleHost : AbstractHost
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string HostLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log-service", "host");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string DevLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log-service", "dev");

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string OpsLogDirectory { get; } = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), Environment.UserInteractive ? "log-interactive" : "log-service", "ops");

    public List<string> ReferenceAssemblyDirectories { get; } = [];
    public ModuleCompilationMode ModuleCompilationMode { get; internal set; } = ModuleCompilationMode.Dynamic;

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

    private string _hostArgumentsDirectory;
    public string HostArgumentsDirectory
    {
        get => _hostArgumentsDirectory;
        set
        {
            _hostArgumentsDirectory = value;
            if (_hostArgumentsDirectory.StartsWith(@".\", StringComparison.InvariantCultureIgnoreCase))
            {
                _hostArgumentsDirectory = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _hostArgumentsDirectory[2..]);
            }
        }
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ConsoleHostFluent.SessionBuilderAction SessionConfigurator { get; internal set; }

    public ConsoleHost(string name)
        : base(name)
    {
        ModulesDirectory = @".\Modules";
        HostArgumentsDirectory = @".\HostArguments";
        ReferenceAssemblyDirectories.Add(@"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\");
        ReferenceAssemblyDirectories.Add(@"C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App\");

        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
    }

    protected override void CustomizeHostBuilder(IHostBuilder builder)
    {
        builder.UseConsoleLifetime();
    }

    protected override ILogger CreateHostLogger()
    {
        var config = new LoggerConfiguration();

        if (SerilogForHostEnabled)
        {
            config = config
                .WriteTo.File(Path.Combine(HostLogDirectory, "host-.txt"),
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

    protected override IExecutionResult RunCustomCommand(string commandId, string[] commandParts)
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

                    var userArguments = new Dictionary<string, string>();

                    var taskNames = commandParts.Skip(2).ToList();
                    var temp = taskNames.ToArray();
                    for (var i = 0; i < temp.Length; i++)
                    {
                        var taskName = temp[i];
                        var idx = taskName.IndexOf('=');
                        if (idx > -1)
                        {
                            if (idx < taskName.Length - 1)
                            {
                                taskNames.Remove(taskName);
                                userArguments[taskName[..idx]] = taskName[(idx + 1)..];
                            }
                            else
                            {
                                taskNames.Remove(taskName);
                                if (i < temp.Length - 1)
                                {
                                    userArguments[taskName[..idx]] = temp[i + 1];
                                    taskNames.Remove(temp[i + 1]);
                                }
                                else
                                {
                                    userArguments[taskName[..idx]] = null;
                                }

                                i++;
                            }
                        }
                    }

                    if (taskNames.Count == 0)
                    {
                        Console.WriteLine("Missing task name(s). Usage: `run <moduleName> <taskNames>`");
                        return new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
                    }

                    return RunModule(commandId, moduleName, taskNames, userArguments);
                }
            case "test-modules":
                var moduleNames = commandParts.Skip(2).ToList();
                if (moduleNames.Count == 0)
                    moduleNames = ModuleLister.GetAllModules(ModulesDirectory);

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

    private IExecutionResult RunModule(string commandId, string moduleName, List<string> taskNames, Dictionary<string, string> userArguments)
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

        var executionResult = ModuleExecuter.Execute(this, commandId, module, [.. taskNames], userArguments);

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
        foreach (var referenceAssemblyDirectory in ReferenceAssemblyDirectories)
        {
            var directory = Directory.GetDirectories(referenceAssemblyDirectory, "8.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();

            Logger.Information("using assemblies from {ReferenceAssemblyDirectory}", directory);

            referenceDllFileNames.AddRange(Directory.GetFiles(directory, "System*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(directory, "Microsoft.AspNetCore*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(directory, "Microsoft.Extensions*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(directory, "Microsoft.Net*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(directory, "netstandard.dll", SearchOption.TopDirectoryOnly));
        }

        var referenceFileNames = referenceDllFileNames
            .Where(x => !Path.GetFileNameWithoutExtension(x).EndsWith("Native", StringComparison.InvariantCultureIgnoreCase))
            .ToList();

        var selfDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        var localDllFileNames = Directory.GetFiles(selfDirectory, "*.dll", SearchOption.TopDirectoryOnly)
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