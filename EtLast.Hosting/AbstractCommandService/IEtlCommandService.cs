namespace FizzCode.EtLast.Hosting;

public interface IEtlCommandService
{
    public string Name { get; }
    public ILogger Logger { get; }

    public void Terminate();
    public CancellationToken CancellationToken { get; }
    public IExecutionResult RunCommand(string source, string commandId, string command, Func<IExecutionResult, Task> resultHandler = null);
    public IExecutionResult RunCommand(string source, string commandId, string[] commandParts, Func<IExecutionResult, Task> resultHandler = null);

    public List<Func<IArgumentCollection, ICommandListener>> CommandListenerCreators { get; }
    public Dictionary<string, string> CommandAliases { get; }
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListeners { get; }
    public TimeSpan MaxTransactionTimeout { get; set; }
    public bool SerilogForModulesDisabled { get; set; }
    public bool SerilogForCommandsEnabled { get; set; }
}