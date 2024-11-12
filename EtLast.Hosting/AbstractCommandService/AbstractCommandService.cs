namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractCommandService : IHostedService, ICommandService
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    [EditorBrowsable(EditorBrowsableState.Never)]
    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ILogger Logger { get; private set; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Microsoft.Extensions.Logging.ILoggerProvider LoggerProvider { get; private set; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string Name { get; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool ModuleLoggingEnabled { get; set; } = true;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool ServiceLoggingEnabled { get; set; } = true;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public TimeSpan MaxTransactionTimeout { get; set; } = TimeSpan.FromHours(4);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<ICommandService, ICommandListener>> CommandListenerCreators { get; } = [];

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Action<ICommandService>> CustomActions { get; } = [];

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListenerCreators { get; } = [];

    protected static readonly Regex QuoteSplitterRegex = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

    public IHostApplicationLifetime HostLifetime { get; set; }

    public ArgumentCollection ServiceArguments { get; private set; }

    protected abstract ILogger CreateServiceLogger();
    protected abstract ArgumentCollection LoadServiceArguments();
    protected abstract IExecutionResult RunCustomCommand(string commandId, string originalCommand, string[] commandParts);
    protected abstract IExecutionResult RunTasksInModuleByNameInternal(bool useAppDomain, string commandId, string moduleName, List<string> taskNames, Dictionary<string, string> userArguments, Dictionary<string, object> argumentOverrides);
    protected abstract IExecutionResult RunTasksInModuleInternal(bool useAppDomain, string commandId, string originalCommand, string moduleName, List<IEtlTask> tasks, Dictionary<string, string> userArguments, Dictionary<string, object> argumentOverrides);
    protected abstract IExecutionResult RunTasks(string commandId, string moduleName, StartupDelegate buildSession, List<IEtlTask> tasks, ArgumentCollection arguments);
    protected abstract IExecutionResult TestModulesInternal(List<string> moduleNames);

    protected abstract void ListCommands();
    protected abstract void ListModules();

    public abstract bool ConsoleHidden { get; }

    private int _activeCommandCounter;

    private readonly CancellationTokenSource CommandListenerTerminationTokenSource = new();

    private readonly ConcurrentDictionary<int, ICommandListener> commandListeners = [];

    protected AbstractCommandService(string name)
    {
        Name = name;
    }

    private void StartCommandListeners()
    {
        var commandLineArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
        if (commandLineArgs.Length > 0)
        {
            Logger.Debug("command line arguments: {CommandLineArguments}", commandLineArgs);

            var result = RunCommand("command line arguments", Guid.CreateVersion7().ToString(), string.Join(' ', commandLineArgs), commandLineArgs).Status;

            if (Debugger.IsAttached)
            {
                Console.WriteLine();
                Console.WriteLine("done, press any key to continue");
                Console.ReadKey();
            }

            return;
        }

        ListCommands();
        ListModules();

        try
        {
            ServiceArguments = LoadServiceArguments();
            if (ServiceArguments == null)
            {
                Logger.Write(LogEventLevel.Fatal, "unexpected exception while compiling service arguments");
                return;
            }
        }
        catch (Exception ex)
        {
            Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception while compiling service arguments");
            return;
        }

        SetMaxTransactionTimeout(MaxTransactionTimeout);

        foreach (var action in CustomActions)
        {
            action.Invoke(this);
        }

        foreach (var creator in CommandListenerCreators)
        {
            var listener = creator.Invoke(this);
            if (listener == null)
                continue;

            ServiceArguments.Inject(listener);

            var thread = new Thread(() =>
            {
                try
                {
                    commandListeners[Environment.CurrentManagedThreadId] = listener;
                    listener.Listen(this, CommandListenerTerminationTokenSource.Token);
                    commandListeners.TryRemove(Environment.CurrentManagedThreadId, out var l);
                }
                catch (Exception ex)
                {
                    Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception happened in command line listener");
                }
            });

            thread.Start();
        }
    }

    protected virtual void InsideMainLoop()
    {
    }

    public void Terminate()
    {
        _cancellationTokenSource.Cancel();
    }

    public void StopAllCommandListeners()
    {
        if (!commandListeners.IsEmpty && !CommandListenerTerminationTokenSource.IsCancellationRequested)
        {
            Logger.Write(LogEventLevel.Information, "stopping all command listeners...");
            CommandListenerTerminationTokenSource.Cancel();
        }

        foreach (var sharedService in _sharedServices.Values)
        {
            try
            {
                sharedService.Stop();
            }
            catch (Exception) { }
        }

        _sharedServices.Clear();
    }

    public IExecutionResult RunTasksInModuleByName(bool useAppDomain, string source, string commandId, string moduleName, List<string> taskNames, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        Logger.Information("module execution command {CommandId} started by {CommandSource}", commandId, source);
        var result = RunTasksInModuleByNameInternal(useAppDomain, commandId, moduleName, taskNames, userArguments: null, argumentOverrides);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        Logger.Information("module execution command {CommandId} finished, active command count: {CommandCount}", commandId, _activeCommandCounter);
        return result;
    }

    public IExecutionResult RunTasksInModule(bool useAppDomain, string source, string commandId, string moduleName, List<IEtlTask> tasks, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        Logger.Information("module execution command {CommandId} started by {CommandSource}", commandId, source);
        var result = RunTasksInModuleInternal(useAppDomain, commandId, null, moduleName, tasks, userArguments: null, argumentOverrides);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        Logger.Information("module execution command {CommandId} finished, active command count: {CommandCount}", commandId, _activeCommandCounter);
        return result;
    }

    public IExecutionResult RunTasks(string source, string commandId, StartupDelegate buildSession, List<IEtlTask> tasks, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        Logger.Information("command {CommandId} started by {CommandSource}", commandId, source);

        var arguments = new ArgumentCollection(ServiceArguments, overrides: argumentOverrides);
        var result = RunTasks(commandId, moduleName: null, buildSession, tasks, arguments);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        Logger.Information("command {CommandId} finished, active command count: {CommandCount}", commandId, _activeCommandCounter);
        return result;
    }

    public IExecutionResult RunCommand(string source, string commandId, string command, Func<IExecutionResult, Task> resultHandler = null)
    {
        var commandParts = QuoteSplitterRegex
            .Matches(command.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommand(source, commandId, command, commandParts, resultHandler);
    }

    public IExecutionResult RunCommand(string source, string commandId, string originalCommand, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        Logger.Information("generic command {CommandId} started by {CommandSource}: {Command}", commandId, source, string.Join(' ', commandParts));

        if (commandParts?.Length >= 1 && CommandAliases.TryGetValue(commandParts[0], out var alias))
        {
            commandParts = QuoteSplitterRegex
                .Matches(alias.Trim())
                .Select(x => x.Value)
                .Concat(commandParts.Skip(1))
                .ToArray();

            originalCommand = string.Join(' ', commandParts);
        }

        IExecutionResult result = null;
        try
        {
            var cmd = commandParts[0].ToLowerInvariant();
            switch (cmd)
            {
                case "stop":
                    StopAllCommandListeners();
                    result = new ExecutionResult(ExecutionStatusCode.Success);
                    break;
                case "exit":
                    Terminate();
                    result = new ExecutionResult(ExecutionStatusCode.Success);
                    break;
                case "help":
                    ListCommands();
                    result = new ExecutionResult(ExecutionStatusCode.Success);
                    break;
                case "list-modules":
                    ListModules();
                    result = new ExecutionResult(ExecutionStatusCode.Success);
                    break;
                case "run":
                case "rundomain":
                    {
                        var moduleName = commandParts.Skip(1).FirstOrDefault();
                        if (string.IsNullOrEmpty(moduleName))
                        {
                            Console.WriteLine("Missing module name. Usage: `" + cmd + " <moduleName> <taskNames>`");
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
                            result = new ExecutionResult(ExecutionStatusCode.CommandArgumentError);
                        }
                        else
                        {
                            result = RunTasksInModuleByNameInternal(useAppDomain: cmd == "runad",
                                commandId, moduleName, taskNames, userArguments, argumentOverrides: null);
                        }
                        break;
                    }
                case "test-modules":
                    var moduleNames = commandParts.Skip(2).ToList();
                    result = TestModulesInternal(moduleNames);
                    break;
                default:
                    result = RunCustomCommand(commandId, originalCommand, commandParts);
                    break;
            }
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatWithEtlDetails();
            Logger.Write(LogEventLevel.Fatal, "unexpected error during execution of command {CommandId}: {ErrorMessage}", commandId, formattedMessage);

            result = new ExecutionResult(ExecutionStatusCode.UnexpectedError);
        }

        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        Logger.Information("generic command {CommandId} finished, active command count: {CommandCount}", commandId, _activeCommandCounter);
        return result;
    }

    private void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
    {
        if (e?.ExceptionObject is not Exception ex)
            return;

        var formattedMessage = ex.FormatWithEtlDetails();

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

    private Thread _mainThread;

    protected virtual void AfterInit()
    {
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Logger = CreateServiceLogger();
        LoggerProvider = new SerilogLoggerProvider(Logger);

        AppDomain.MonitoringIsEnabled = true;
        AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

        if (Environment.UserInteractive)
        {
            Logger.Information("EtLast service started: {ProgramName} {HostVersion}", Name, Assembly.GetEntryAssembly().GetName().Version.ToString());
            Logger.Debug("Environment:");
            Logger.Debug("  {0,-23} = {1}", "EtLast", typeof(IEtlContext).Assembly.GetName().Version.ToString());
            Logger.Debug("  {0,-23} = {1}", "Instance", Environment.MachineName);
            Logger.Debug("  {0,-23} = {1}", "UserName", Environment.UserName);
            Logger.Debug("  {0,-23} = {1}", "UserDomainName", Environment.UserDomainName);
            Logger.Debug("  {0,-23} = {1}", "OSVersion", Environment.OSVersion);
            Logger.Debug("  {0,-23} = {1}", "ProcessorCount", Environment.ProcessorCount);
            Logger.Debug("  {0,-23} = {1}", "UserInteractive", Environment.UserInteractive);
            Logger.Debug("  {0,-23} = {1}", "Is64Bit", Environment.Is64BitProcess);
            Logger.Debug("  {0,-23} = {1}", "IsPrivileged", Environment.IsPrivilegedProcess);
            Logger.Debug("  {0,-23} = {1}", "TickCountSinceStartup", Environment.TickCount64);
        }
        else
        {
            Logger.Information("EtLast service started: {ProgramName} {ProgramVersion}", Name, Assembly.GetEntryAssembly().GetName().Version.ToString());
        }

        AfterInit();

        StartCommandListeners();

        _mainThread = new Thread(() =>
        {
            while (!commandListeners.IsEmpty)
            {
                InsideMainLoop();
                Thread.Sleep(100);
            }

            if (_activeCommandCounter > 0)
            {
                Logger.Write(LogEventLevel.Information, "waiting for {CommandCount} commands to finish, before service is terminated...",
                    _activeCommandCounter);

                while (_activeCommandCounter != 0)
                {
                    Thread.Sleep(100);
                }
            }

            Logger.Write(LogEventLevel.Debug, "EtLast service stopped");
            HostLifetime?.StopApplication();
        });

        _mainThread.Start();

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        StopAllCommandListeners();

        if (_mainThread.ThreadState != System.Threading.ThreadState.Stopped)
        {
            Logger.Write(LogEventLevel.Debug, "waiting for EtLast to finish");
            _mainThread.Join();
        }

        return Task.CompletedTask;
    }

    private readonly Dictionary<Type, ISharedCommandService> _sharedServices = [];

    public T SharedService<T>()
        where T : ISharedCommandService, new()
    {
        lock (_sharedServices)
        {
            if (_sharedServices.TryGetValue(typeof(T), out var service))
                return (T)service;

            var newService = new T();
            ServiceArguments.Inject(newService);

            newService.Start(this);
            _sharedServices.Add(typeof(T), newService);

            return newService;
        }
    }
}