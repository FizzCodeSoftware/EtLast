namespace FizzCode.EtLast.ConsoleHost;

public interface IHostBuilder
{
    public IHost Result { get; }
    public IHost Build();
}