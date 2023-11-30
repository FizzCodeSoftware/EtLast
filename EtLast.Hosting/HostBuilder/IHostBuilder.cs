namespace FizzCode.EtLast.Hosting;

public interface IHostBuilder
{
    public IHost Result { get; }
    public IHost Build();
}