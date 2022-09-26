namespace FizzCode.EtLast;

public interface IPipeBuilder
{
    public IPipeBuilder OnSuccess(Func<Pipe, Action> processCreator);
    public IPipeBuilder OnSuccess(Func<Pipe, IProcess> processCreator);
    public IPipeBuilder OnSuccess<T>(out T result, Func<Pipe, T> processCreator) where T : IProcess;
    public IPipeBuilder IsolatedPipe(Action<Pipe, IPipeStarter> handler);
    public IPipeBuilder OnError(Func<Pipe, IProcess> processCreator);
    public void ThrowOnError();
}