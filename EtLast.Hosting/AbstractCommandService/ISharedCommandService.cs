namespace FizzCode.EtLast;

public interface ISharedCommandService
{
    public ICommandService CommandService { get; }
    public void Start(ICommandService commandService);
    public void Stop();
}