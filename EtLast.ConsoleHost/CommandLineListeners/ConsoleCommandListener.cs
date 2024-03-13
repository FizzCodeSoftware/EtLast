using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

public class ConsoleCommandListener : ICommandListener
{
    public void Listen(IEtlHost host, CancellationToken cancellationToken)
    {
        var commands = new List<string>();
        var lck = new object();

        if (Environment.UserInteractive)
        {
            var thread = new Thread(ListenForNewCommand(host, commands, lck, cancellationToken))
            {
                IsBackground = true,
            };

            thread.Start();
        }

        if (Environment.UserInteractive)
        {
            host.Logger.Write(LogEventLevel.Information, "listening on console");
        }
        else
        {
            host.Logger.Write(LogEventLevel.Information, "listening on console (non-interactive mode)");
        }

        while (!host.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            string command = null;
            lock (lck)
            {
                if (commands.Count > 0)
                {
                    command = commands[0];
                    commands.RemoveAt(0);
                }
            }

            if (command != null)
            {
                host.Logger.Write(LogEventLevel.Information, "executing command entered on the console: {Command}", command);
                var result = host.RunCommand(Guid.NewGuid().ToString(), command);
                host.Logger.Write(LogEventLevel.Information, "command result {CommandResult}", result.Status.ToString());
            }
            else
            {
                Thread.Sleep(10);
            }
        }

        //Console.WriteLine("listening on console finished: " + thread.ThreadState.ToString());
    }

    private ThreadStart ListenForNewCommand(IEtlHost host, List<string> commands, object lck, CancellationToken cancellationToken)
    {
        return () =>
        {
            while (!host.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var command = Console.ReadLine();
                    if (!string.IsNullOrEmpty(command))
                    {
                        lock (lck)
                        {
                            commands.Add(command);
                        }
                    }
                }
                catch (Exception ex)
                {
                    host.Logger.Write(LogEventLevel.Error, ex, "unexpected error in {CommandListenerName}", GetType().Name);
                }
            }
        };
    }
}