namespace FizzCode.EtLast.Hosting;

public interface ICommandService
{
    public string Name { get; }
    public ILogger Logger { get; }

    public void Terminate();
    public CancellationToken CancellationToken { get; }
    public IExecutionResult RunCommand(string source, string commandId, string command, Func<IExecutionResult, Task> resultHandler = null);
    public IExecutionResult RunCommand(string source, string commandId, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null);

    public List<Func<IArgumentCollection, ICommandListener>> CommandListenerCreators { get; }
    public Dictionary<string, string> CommandAliases { get; }
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListenerCreators { get; }
    public TimeSpan MaxTransactionTimeout { get; set; }
    public bool ModuleLoggingEnabled { get; set; }
    public bool ServiceLoggingEnabled { get; set; }
}