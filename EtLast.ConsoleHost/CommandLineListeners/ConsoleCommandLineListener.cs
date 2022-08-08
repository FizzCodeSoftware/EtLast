namespace FizzCode.EtLast.ConsoleHost;

public class ConsoleCommandLineListener : ICommandLineListener
{
    public void Listen(IHost host)
    {
        var commands = new List<string>();
        var lck = new object();

        var thread = new Thread(ListenForNewCommand(host, commands, lck))
        {
            IsBackground = true
        };
        thread.Start();

        Console.WriteLine("listening on console");
        while (!host.CancellationToken.IsCancellationRequested)
        {
            string commandLine = null;
            lock (lck)
            {
                if (commands.Count > 0)
                {
                    commandLine = commands[0];
                    commands.RemoveAt(0);
                }
            }
            Thread.Sleep(10);

            if (commandLine != null)
            {
                var result = host.RunCommandLine(commandLine);
                Console.WriteLine("command commandLine: " + result.Status.ToString());
            }
        }

        Console.WriteLine("listening on console finished: " + thread.ThreadState.ToString());
    }

    private static ThreadStart ListenForNewCommand(IHost host, List<string> commands, object lck)
    {
        return () =>
        {
            while (!host.CancellationToken.IsCancellationRequested)
            {
                var commandLine = Console.ReadLine();
                if (!string.IsNullOrEmpty(commandLine))
                {
                    lock (lck)
                    {
                        commands.Add(commandLine);
                    }
                }
            }
        };
    }
}