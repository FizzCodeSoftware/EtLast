namespace FizzCode.EtLast
{
    public interface IExecutionContext
    {
        public string SessionId { get; }
        public IExecutionContext ParentContext { get; }
        public ITopic Topic { get; }
        public string Name { get; }
    }
}