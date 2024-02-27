using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractHost : IHost
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    [EditorBrowsable(EditorBrowsableState.Never)]
    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public ILogger Logger { get; private set; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string Name { get; }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool SerilogForModulesDisabled { get; set; } = true;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool SerilogForHostEnabled { get; set; } = true;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public TimeSpan MaxTransactionTimeout { get; set; } = TimeSpan.FromHours(4);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public Dictionary<string, string> CommandAliases { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<IArgumentCollection, ICommandListener>> CommandListenerCreators { get; } = [];

    [EditorBrowsable(EditorBrowsableState.Never)]
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListeners { get; } = [];

    protected static readonly Regex QuoteSplitterRegex = new("(?<=\")[^\"]*(?=\")|[^\" ]+");

    protected AbstractHost(string name)
    {
        Name = name;
    }

    protected abstract ILogger CreateHostLogger();
    protected abstract IArgumentCollection LoadHostArguments();
    protected abstract IExecutionResult RunCustomCommand(string[] commandParts);

    protected abstract void ListCommands();
    protected abstract void ListModules();

    private int _activeCommandCounter;

    public ExecutionStatusCode Run()
    {
        Logger = CreateHostLogger();

        AppDomain.MonitoringIsEnabled = true;
        AppDomain.CurrentDomain.UnhandledException -= UnhandledExceptionHandler;
        AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

        Console.WriteLine();
        Logger.Information("{ProgramName} {ProgramVersion} started", Name, Assembly.GetEntryAssembly().GetName().Version.ToString());

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

        ListCommands();
        ListModules();

        IArgumentCollection hostArguments = null;
        try
        {
            hostArguments = LoadHostArguments();
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

        var gracefulTerminationTokenSource = new CancellationTokenSource();
        var threads = new List<Thread>();
        var listeners = new List<ICommandListener>();
        foreach (var creator in CommandListenerCreators)
        {
            var listener = creator.Invoke(hostArguments);
            if (listener == null)
                continue;

            var thread = new Thread(() =>
            {
                try
                {
                    listener.Listen(this, gracefulTerminationTokenSource.Token);
                    listeners.Add(listener);
                }
                catch (Exception ex)
                {
                    Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception happened in command line listener");
                }
            });

            threads.Add(thread);
            thread.Start();
        }

        while (true)
        {
            if (threads.All(x => x.ThreadState != System.Threading.ThreadState.Running))
            {
                Logger.Write(LogEventLevel.Information, "all command listener threads stopped, terminating host...");
                break;
            }

            var semaphoreOn = false;
            // todo: check semaphore

            if (semaphoreOn)
                gracefulTerminationTokenSource.Cancel();

            Thread.Sleep(100);
        }

        var runningThreadCount = threads.Count(x => x.ThreadState == System.Threading.ThreadState.Running);
        if (runningThreadCount > 0)
        {
            Logger.Write(LogEventLevel.Information, "waiting for {ThreadCount} command listener threads to stop, before host is terminated...", runningThreadCount);

            foreach (var thread in threads)
                thread.Join();
        }

        if (_activeCommandCounter > 0)
        {
            Logger.Write(LogEventLevel.Information, "waiting for {CommandCount} commands to finish, before host is terminated...", _activeCommandCounter);
            while (_activeCommandCounter != 0)
            {
                Thread.Sleep(100);
            }
        }

        return ExecutionStatusCode.Success;
    }

    public void Terminate()
    {
        _cancellationTokenSource.Cancel();
    }

    public static void StopGracefully()
    {
        // todo: trigger semaphore
        // todo: block caller thread until all processes are gone
    }

    public IExecutionResult RunCommand(string command, Func<IExecutionResult, System.Threading.Tasks.Task> resultHandler = null)
    {
        var commandParts = QuoteSplitterRegex
            .Matches(command.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommand(commandParts, resultHandler);
    }

    public IExecutionResult RunCommand(string[] commandParts, Func<IExecutionResult, System.Threading.Tasks.Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        var result = RunCommandInternal(commandParts);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        return result;
    }

    private IExecutionResult RunCommandInternal(string[] commandParts)
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
                    StopGracefully();
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

            return RunCustomCommand(commandParts);
        }
        catch (Exception ex)
        {
            var formattedMessage = ex.FormatExceptionWithDetails();
            Logger.Write(LogEventLevel.Fatal, "unexpected error during execution: {ErrorMessage}", formattedMessage);

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
}