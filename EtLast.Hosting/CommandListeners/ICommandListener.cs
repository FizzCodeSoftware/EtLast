using FizzCode.EtLast.Hosting;

namespace FizzCode.EtLast;

public interface ICommandListener
{
    public void Listen(ICommandService commandService, CancellationToken cancellationToken);
}