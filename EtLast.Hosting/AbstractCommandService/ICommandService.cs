namespace FizzCode.EtLast.Hosting;

public delegate void StartupDelegate(ISessionBuilder builder, IArgumentCollection arguments);

public interface ICommandService
{
    public string Name { get; }
    public ILogger Logger { get; }
    public Microsoft.Extensions.Logging.ILoggerProvider LoggerProvider { get; }

    public IArgumentCollection ServiceArguments { get; }

    public void Terminate();
    public CancellationToken CancellationToken { get; }

    public IExecutionResult RunTasksInModuleByName(bool useAppDomain, string source, string commandId, string moduleName, List<string> taskNames, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null);
    public IExecutionResult RunTasksInModule(bool useAppDomain, string source, string commandId, string moduleName, List<IEtlTask> tasks, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null);
    public IExecutionResult RunTasks(string source, string commandId, StartupDelegate startup, List<IEtlTask> tasks, Dictionary<string, object> argumentOverrides = null, Func<IExecutionResult, Task> resultHandler = null);

    public IExecutionResult RunCommand(string source, string commandId, string command, Func<IExecutionResult, Task> resultHandler = null);
    public IExecutionResult RunCommand(string source, string commandId, string originalCommand, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null);

    public List<Func<ICommandService, IArgumentCollection, ICommandListener>> CommandListenerCreators { get; }
    public Dictionary<string, string> CommandAliases { get; }
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListenerCreators { get; }
    public TimeSpan MaxTransactionTimeout { get; set; }
    public bool ModuleLoggingEnabled { get; set; }
    public bool ServiceLoggingEnabled { get; set; }
}