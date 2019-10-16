namespace FizzCode.EtLast
{
    using System.Threading;

    public abstract class AbstractJob : IJob
    {
        public string Name { get; set; }
        public IfJobDelegate If { get; set; }
        public IProcess Process { get; private set; }

        protected AbstractJob()
        {
            Name = TypeHelpers.GetFriendlyTypeName(GetType());
        }

        public abstract void Execute(CancellationTokenSource cancellationTokenSource);

        public void SetProcess(IProcess process)
        {
            Process = process;
        }
    }
}