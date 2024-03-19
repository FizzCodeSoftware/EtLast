using FizzCode.EtLast.Hosting;

namespace FizzCode.EtLast;

public class ConsoleCommandListener : ICommandListener
{
    public void Listen(IEtlCommandService commandService, CancellationToken cancellationToken)
    {
        var commands = new List<string>();
        var lck = new object();

        if (Environment.UserInteractive)
        {
            var thread = new Thread(ListenForNewCommand(commandService, commands, lck, cancellationToken))
            {
                IsBackground = true,
            };

            thread.Start();
        }

        if (Environment.UserInteractive)
        {
            commandService.Logger.Write(LogEventLevel.Information, "listening on console");
        }
        else
        {
            commandService.Logger.Write(LogEventLevel.Information, "listening on console (non-interactive mode)");
        }

        while (!commandService.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
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
                commandService.RunCommand("console", Guid.NewGuid().ToString(), command);
            }
            else
            {
                Thread.Sleep(10);
            }
        }

        //Console.WriteLine("listening on console finished: " + thread.ThreadState.ToString());
    }

    private ThreadStart ListenForNewCommand(IEtlCommandService host, List<string> commands, object lck, CancellationToken cancellationToken)
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