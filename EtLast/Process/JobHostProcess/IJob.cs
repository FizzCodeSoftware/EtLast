namespace FizzCode.EtLast
{
    using System.Threading;

    public interface IJob
    {
        string Name { get; set; }
        IfJobDelegate If { get; }
        void Execute(CancellationTokenSource cancellationTokenSource);

        IProcess Process { get; }
        public void SetProcess(IProcess process);
    }
}