namespace FizzCode.EtLast.ConsoleHost;

public interface IHostBuilder
{
    internal Host Result { get; }
    public IHost Build();
}