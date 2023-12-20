namespace FizzCode.EtLast.Host;

public interface IHost
{
    public string Name { get; }
    public ILogger Logger { get; }

    public void Terminate();
    public CancellationToken CancellationToken { get; }
    public IExecutionResult RunCommand(string command);
    public IExecutionResult RunCommand(string[] commandParts);
    public ExecutionStatusCode Run();

    public List<Func<IArgumentCollection, ICommandListener>> CommandListenerCreators { get; }
    public Dictionary<string, string> CommandAliases { get; }
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListeners { get; }
    public TimeSpan MaxTransactionTimeout { get; set; }
    public bool SerilogForModulesDisabled { get; set; }
    public bool SerilogForHostEnabled { get; set; }
}