namespace FizzCode.EtLast
{
    using System.Threading;

    public interface IJob
    {
        string Name { get; set; }
        IfJobDelegate If { get; }
        void Execute(IProcess process, CancellationTokenSource cancellationTokenSource);
    }
}