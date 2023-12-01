namespace FizzCode.EtLast;

public sealed class ConsoleHostBuilder : IHostBuilder
{
    public static IHostBuilder New(string hostName)
    {
        return new ConsoleHostBuilder(hostName);
    }

    public IHost Result { get; set; }

    internal ConsoleHostBuilder(string name)
    {
        Result = new ConsoleHost(name);
    }

    public IHost Build()
    {
        return Result;
    }
}