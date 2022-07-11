namespace FizzCode.EtLast.ConsoleHost;

public class ConsoleCommandLineListener : ICommandLineListener
{
    public void Listen(IHost host)
    {
        Console.WriteLine("listening on console");
        while (!host.CancellationToken.IsCancellationRequested)
        {
            var commandLine = Console.ReadLine();
            if (string.IsNullOrEmpty(commandLine))
                continue;

            var result = host.RunCommandLine(commandLine);

            Console.WriteLine("command result: " + result.Status.ToString());
        }
    }
}