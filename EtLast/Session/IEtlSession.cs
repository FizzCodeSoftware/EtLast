namespace FizzCode.EtLast
{
    public interface IEtlSession
    {
        public string Id { get; }
        public IEtlContext Context { get; }

        public T Service<T>() where T : IEtlService, new();

        public bool Success { get; }
        public TaskResult ExecuteTask(IProcess caller, IEtlTask task);
        public TaskResult ExecuteProcess(IProcess caller, IExecutable process);
    }
}