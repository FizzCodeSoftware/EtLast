using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

public interface ICommandListener
{
    public void Listen(IEtlHost host, CancellationToken cancellationToken);
}