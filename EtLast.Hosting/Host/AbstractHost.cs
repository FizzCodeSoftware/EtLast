using FizzCode.EtLast.Host;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractHost : BackgroundService, IEtlHost
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

    public IHostApplicationLifetime Lifetime { get; private set; }

    protected AbstractHost(string name)
    {
        Name = name;
    }

    protected abstract ILogger CreateHostLogger();
    protected abstract IArgumentCollection LoadHostArguments();
    protected abstract IExecutionResult RunCustomCommand(string commandId, string[] commandParts);

    protected abstract void ListCommands();
    protected abstract void ListModules();

    private int _activeCommandCounter;

    protected CancellationTokenSource GracefulTerminationTokenSource { get; } = new CancellationTokenSource();

    private readonly List<Thread> threads = [];
    private readonly List<ICommandListener> listeners = [];

    private void Start()
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

            var result = RunCommand(Guid.NewGuid().ToString(), commandLineArgs).Status;

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

        IArgumentCollection hostArguments = null;
        try
        {
            hostArguments = LoadHostArguments();
            if (hostArguments == null)
            {
                Logger.Write(LogEventLevel.Fatal, "unexpected exception while compiling host arguments");
                return;
            }
        }
        catch (Exception ex)
        {
            Logger.Write(LogEventLevel.Fatal, ex, "unexpected exception while compiling host arguments");
            return;
        }

        SetMaxTransactionTimeout(MaxTransactionTimeout);

        foreach (var creator in CommandListenerCreators)
        {
            var listener = creator.Invoke(hostArguments);
            if (listener == null)
                continue;

            var thread = new Thread(() =>
            {
                try
                {
                    listener.Listen(this, GracefulTerminationTokenSource.Token);
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
    }

    private async Task<ExecutionStatusCode> Execute()
    {
        while (true)
        {
            if (!threads.Any(x => x.ThreadState is System.Threading.ThreadState.Running or System.Threading.ThreadState.WaitSleepJoin))
            {
                Logger.Write(LogEventLevel.Information, "all command listener threads stopped, terminating host...");
                break;
            }

            InsideMainLoop();

            Thread.Sleep(100);
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

    protected virtual void InsideMainLoop()
    {

    }

    public void Terminate()
    {
        _cancellationTokenSource.Cancel();
    }

    public void StopGracefully()
    {
        if (!GracefulTerminationTokenSource.IsCancellationRequested)
        {
            Logger.Write(LogEventLevel.Information, "gracefully stopping...");
            GracefulTerminationTokenSource.Cancel();

            Lifetime?.StopApplication();
        }
    }

    public IExecutionResult RunCommand(string commandId, string command, Func<IExecutionResult, Task> resultHandler = null)
    {
        var commandParts = QuoteSplitterRegex
            .Matches(command.Trim())
            .Select(x => x.Value)
            .ToArray();

        return RunCommand(commandId, commandParts, resultHandler);
    }

    public IExecutionResult RunCommand(string commandId, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null)
    {
        Interlocked.Increment(ref _activeCommandCounter);
        var result = RunCommandInternal(commandId, commandParts);
        resultHandler?.Invoke(result)?.Wait();
        Interlocked.Decrement(ref _activeCommandCounter);
        return result;
    }

    private IExecutionResult RunCommandInternal(string commandId, string[] commandParts)
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

            return RunCustomCommand(commandId, commandParts);
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

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        Start();
        CustomStart();
        return base.StartAsync(cancellationToken);
    }

    protected virtual void CustomStart()
    {
    }

    public async Task Run()
    {
        var builder = new HostBuilder()
            .ConfigureServices((ctx, svc) => svc
                .AddHostedService(sp => this)
            );

        CustomizeHostBuilder(builder);

        var appHost = builder.Build();
        Lifetime = appHost.Services.GetRequiredService<IHostApplicationLifetime>();
        await appHost.RunAsync();
    }

    protected virtual void CustomizeHostBuilder(IHostBuilder builder)
    {
        builder.UseConsoleLifetime();
    }

#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
    {
        stoppingToken.Register(() => StopGracefully());
        await Execute();
        Console.WriteLine("execute is over");
        Lifetime?.StopApplication();
    });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods
}