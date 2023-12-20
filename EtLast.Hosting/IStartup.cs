namespace FizzCode.EtLast;

public interface IStartup
{
    public void Configure(HostSessionSettings settings, IArgumentCollection arguments);
}
