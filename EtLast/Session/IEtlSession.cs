namespace FizzCode.EtLast;

public interface IEtlSession
{
    public string Id { get; }
    public IEtlContext Context { get; }

    public T Service<T>() where T : IEtlService, new();

    public bool Success { get; }
    public TaskResult<T> ExecuteTask<T>(IProcess caller, T task) where T : IEtlTask;
    public ProcessResult ExecuteProcess(IProcess caller, IExecutable process);
}
