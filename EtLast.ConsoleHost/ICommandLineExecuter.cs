namespace FizzCode.EtLast.ConsoleHost;

public interface IHost
{
    public string ProgramName { get; }
    public CancellationToken CancellationToken { get; }
    public IExecutionResult RunCommandLine(string commandLine);
    public IExecutionResult RunCommandLine(string[] commandLineParts);
    public ExecutionStatusCode Run();
}