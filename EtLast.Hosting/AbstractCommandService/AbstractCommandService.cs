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
    public List<Func<ICommandService, IArgumentCollection, ICommandListener>> CommandListenerCreators { get; } = [];

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListenerCreators { get; } = [];

    protected static readonly Regex QuoteSplitterRegex = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

    public IHostApplicationLifetime HostLifetime { get; set; }

    protected AbstractCommandService(string name)
    {
        Name = name;
    }

    protected abstract ILogger CreateServiceLogger();
    protected abstract IArgumentCollection LoadServiceArguments();
    protected abstract IExecutionResult RunCustomCommand(string commandId, string originalCommand, string[] commandParts);

    protected abstract void ListCommands();
    protected abstract void ListModules();

    private int _activeCommandCounter;

    protected CancellationTokenSource CommandListenerTerminationTokenSource { get; } = new CancellationTokenSource();

    private readonly ConcurrentDictionary<int, ICommandListener> commandListeners = [];

    private void StartCommandListeners()
    {
        var commandLineArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
        if (commandLineArgs.Length > 0)
        {
            Logger.Debug("command line arguments: {CommandLineArguments}", commandLineArgs);

            var result = RunCommand("command line arguments", Guid.NewGuid().ToString(), string.Join(' ', commandLineArgs), commandLineArgs).Status;

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

        IArgumentCollection serviceArguments = null;
        try
        {
            serviceArguments = LoadServiceArguments();
            if (serviceArguments == null)
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

        foreach (var creator in CommandListenerCreators)
        {
            var listener = creator.Invoke(this, serviceArguments);
            if (listener == null)
                continue;

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
    }

    public IExecutionResult RunCommand(string source, string commandId, string originalCommand, Func<IExecutionResult, Task> resultHandler = null)
    {
        var commandParts = QuoteSplitterRegex
            .Matches(originalCommand.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommand(source, commandId, originalCommand, commandParts, resultHandler);
    }

    public IExecutionResult RunCommand(string source, string commandId, string originalCommand, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        Logger.Information("command {CommandId} started by {CommandSource}: {Command}", commandId, source, string.Join(' ', commandParts));
        var result = RunCommandInternal(commandId, originalCommand, commandParts);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        Logger.Information("command {CommandId} finished, active command count: {CommandCount}", commandId, _activeCommandCounter);
        return result;
    }

    private IExecutionResult RunCommandInternal(string commandId, string originalCommand, string[] commandParts)
    {
        if (commandParts?.Length >= 1 && CommandAliases.TryGetValue(commandParts[0], out var alias))
        {
            commandParts = QuoteSplitterRegex
                .Matches(alias.Trim())
                .Select(x => x.Value)
                .Concat(commandParts.Skip(1))
                .ToArray();
        }

        try
        {
            switch (commandParts[0].ToLowerInvariant())
            {
                case "stop":
                    StopAllCommandListeners();
                    return new ExecutionResult(ExecutionStatusCode.Success);
                case "exit":
                    Terminate();
                    return new ExecutionResult(ExecutionStatusCode.Success);
                case "help":
                    ListCommands();
                    return new ExecutionResult(ExecutionStatusCode.Success);
                case "list-modules":
                    ListModules();
                    return new ExecutionResult(ExecutionStatusCode.Success);
            }

            return RunCustomCommand(commandId, originalCommand, commandParts);
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            Logger.Write(LogEventLevel.Fatal, "unexpected error during execution of command {CommandId}: {ErrorMessage}", commandId, formattedMessage);

            return new ExecutionResult(ExecutionStatusCode.UnexpectedError);
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
            Logger.Information("EtLast service started: {ProgramName} {ProgramVersion}", Name, Assembly.GetEntryAssembly().GetName().Version.ToString());
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
}