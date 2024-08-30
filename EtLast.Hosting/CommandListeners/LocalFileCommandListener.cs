namespace FizzCode.EtLast;

public class LocalFileCommandListener : ICommandListener
{
    public required string CommandFilePath { get; init; }

    public void Listen(ICommandService commandService, CancellationToken cancellationToken)
    {
        commandService.Logger.Write(LogEventLevel.Information, "listening the following file for commands: " + CommandFilePath);

        while (!commandService.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
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
                commandService.RunCommand("file", Guid.NewGuid().ToString("D"), command);
            }
            else
            {
                Thread.Sleep(10);
            }
        }
    }
}