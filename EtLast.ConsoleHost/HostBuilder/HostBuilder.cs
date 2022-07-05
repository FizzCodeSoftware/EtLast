namespace FizzCode.EtLast.ConsoleHost;

public sealed class HostBuilder : IHostBuilder
{
    public static IHostBuilder New(string hostName)
    {
        return new HostBuilder(hostName);
    }

    public Host Result { get; set; }

    internal HostBuilder(string name)
    {
        Result = new Host(name);
    }

    public IHost Build()
    {
        return Result;
    }
}