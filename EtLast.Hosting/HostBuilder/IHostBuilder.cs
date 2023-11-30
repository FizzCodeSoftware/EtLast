namespace FizzCode.EtLast;

public interface IHostBuilder
{
    public IHost Result { get; }
    public IHost Build();
}