namespace FizzCode.EtLast.Hosting;

public interface IHost
{
    public string ProgramName { get; }
    public ILogger HostLogger { get; }

    public void Terminate();
    public CancellationToken CancellationToken { get; }
    public IExecutionResult RunCommandLine(string commandLine);
    public IExecutionResult RunCommandLine(string[] commandLineParts);
    public ExecutionStatusCode Run();

    public string[] CommandLineArgs { get; set; }
    public List<Func<IArgumentCollection, ICommandLineListener>> CommandLineListenerCreators { get; }
    public Dictionary<string, string> CommandAliases { get; }
    public List<Func<IEtlContext, IEtlContextListener>> EtlContextListeners { get; }
    public string HostArgumentsFolder { get; set; }
    public TimeSpan MaxTransactionTimeout { get; set; }
    public string ModulesFolder { get; set; }
    public List<string> ReferenceAssemblyFolders { get; }
    public bool SerilogForModulesEnabled { get; set; }
    public bool SerilogForHostEnabled { get; set; }
    public ModuleCompilationMode ModuleCompilationMode { get; set; }
}