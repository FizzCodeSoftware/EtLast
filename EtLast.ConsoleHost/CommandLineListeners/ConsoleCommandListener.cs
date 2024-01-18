﻿using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

public class ConsoleCommandListener : ICommandListener
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
                var result = host.RunCommand(command);
                Console.WriteLine("command: " + result.Status.ToString());
            }
            else
            {
                Thread.Sleep(10);
            }
        }

        //Console.WriteLine("listening on console finished: " + thread.ThreadState.ToString());
    }

    private static ThreadStart ListenForNewCommand(IHost host, List<string> commands, object lck)
    {
        return () =>
        {
            while (!host.CancellationToken.IsCancellationRequested)
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
                catch (Exception)
                {
                }
            }
        };
    }
}