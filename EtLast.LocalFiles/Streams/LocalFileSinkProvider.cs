namespace FizzCode.EtLast;

public enum LocalSinkFileExistsAction { Continue, Exception, DeleteAndContinue }

[ContainsProcessParameterValidation]
public class LocalFileSinkProvider : ISinkProvider
{
    /// <summary>
    /// Generates file name based on a partition key.
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required Func<string, string> FileNameGenerator { get; init; }

    /// <summary>
    /// Default value is <see cref="LocalSinkFileExistsAction.Exception"/>.
    /// </summary>
    public required LocalSinkFileExistsAction ActionWhenFileExists { get; init; } = LocalSinkFileExistsAction.Exception;

    /// <summary>
    /// Default value is <see cref="FileMode.Append"/>.
    /// </summary>
    public required FileMode FileMode { get; init; } = FileMode.Append;

    /// <summary>
    /// Default value is <see cref="FileAccess.Write"/>.
    /// </summary>
    public FileAccess FileAccess { get; init; } = FileAccess.Write;

    /// <summary>
    /// Default value is <see cref="FileShare.Read"/>.
    /// </summary>
    public FileShare FileShare { get; init; } = FileShare.Read;

    public bool AutomaticallyDispose => true;

    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat)
    {
        var fileName = FileNameGenerator.Invoke(partitionKey);

        var ioCommand = caller.Context.RegisterIoCommandStart(caller, new IoCommand()
        {
            Kind = IoCommandKind.fileWrite,
            Location = Path.GetDirectoryName(fileName),
            Path = Path.GetFileName(fileName),
            Message = "writing to local file",
        });

        if (ActionWhenFileExists != LocalSinkFileExistsAction.Continue && File.Exists(fileName))
        {
            if (ActionWhenFileExists == LocalSinkFileExistsAction.Exception)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", fileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    fileName));

                ioCommand.Exception = exception;
                ioCommand.AffectedDataCount = 0;
                caller.Context.RegisterIoCommandEnd(caller, ioCommand);
                throw exception;
            }
            else if (ActionWhenFileExists == LocalSinkFileExistsAction.DeleteAndContinue)
            {
                try
                {
                    File.Delete(fileName);
                }
                catch (Exception ex)
                {
                    var exception = new LocalFileWriteException(caller, "error while writing local file / file deletion failed", fileName, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, file deletion failed, message: {1}",
                        fileName, ex.Message));
                    exception.Data["FileName"] = fileName;

                    ioCommand.Exception = exception;
                    ioCommand.AffectedDataCount = 0;
                    caller.Context.RegisterIoCommandEnd(caller, ioCommand);
                    throw exception;
                }
            }
        }

        var directory = Path.GetDirectoryName(fileName);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                var exception = new LocalFileWriteException(caller, "error while writing local file / directory creation failed", fileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, directory creation failed, message: {1}",
                    fileName, ex.Message));
                exception.Data["FileName"] = fileName;
                exception.Data["Directory"] = directory;

                ioCommand.Exception = exception;
                ioCommand.AffectedDataCount = 0;
                caller.Context.RegisterIoCommandEnd(caller, ioCommand);
                throw exception;
            }
        }

        try
        {
            var sink = caller.Context.GetSink(Path.GetDirectoryName(fileName), Path.GetFileName(fileName), sinkFormat, caller.GetType());

            var stream = new FileStream(fileName, FileMode, FileAccess, FileShare);
            return new NamedSink(fileName, stream, ioCommand, sink);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileWriteException(caller, "error while writing local file", fileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}",
                fileName, ex.Message));
            exception.Data["FileName"] = fileName;

            ioCommand.Exception = exception;
            ioCommand.AffectedDataCount = 0;
            caller.Context.RegisterIoCommandEnd(caller, ioCommand);
            throw exception;
        }
    }
}