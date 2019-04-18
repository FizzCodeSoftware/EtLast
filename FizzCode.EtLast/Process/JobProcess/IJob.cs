namespace FizzCode.EtLast
{
    using System.Threading;

    public interface IJob
    {
        string Name { get; }
        void Execute(IProcess process, CancellationTokenSource cancellationTokenSource);
    }
}