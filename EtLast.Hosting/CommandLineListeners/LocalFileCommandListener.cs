using FizzCode.EtLast.Host;

namespace FizzCode.EtLast;

public class LocalFileCommandListener : ICommandListener
{
    public required string CommandFilePath { get; init; }

    public void Listen(IEtlHost host, CancellationToken cancellationToken)
    {
        host.Logger.Write(LogEventLevel.Information, "listening the following file for commands: " + CommandFilePath);

        while (!host.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            string command = null;
            if (File.Exists(CommandFilePath))
            {
                try
                {
                    command = File.ReadAllText(CommandFilePath);
                }
                catch { }
            }

            if (command != null)
            {
                File.Move(CommandFilePath, Path.ChangeExtension(CommandFilePath, ".old"));
                host.RunCommand("file", Guid.NewGuid().ToString(), command);
            }
            else
            {
                Thread.Sleep(10);
            }
        }
    }
}