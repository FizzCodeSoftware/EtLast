namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileSinkProvider : IOneSinkProvider
{
    /// <summary>
    /// Generates file name.
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    /// <summary>
    /// Default value is <see cref="LocalSinkFileExistsAction.ThrowException"/>.
    /// </summary>
    public required LocalSinkFileExistsAction ActionWhenFileExists { get; init; }

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

    public NamedSink GetSink(IProcess caller, string sinkFormat, string[] columns)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileWrite,
            Location = System.IO.Path.GetDirectoryName(Path),
            Path = System.IO.Path.GetFileName(Path),
            Message = "writing to local file",
        });

        if (ActionWhenFileExists != LocalSinkFileExistsAction.Continue && File.Exists(Path))
        {
            if (ActionWhenFileExists == LocalSinkFileExistsAction.ThrowException)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", Path);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    Path));

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
            else if (ActionWhenFileExists == LocalSinkFileExistsAction.Overwrite)
            {
                try
                {
                    File.Delete(Path);
                }
                catch (Exception ex)
                {
                    var exception = new LocalFileWriteException(caller, "error while writing local file / file deletion failed", Path, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, file deletion failed, message: {1}",
                        Path, ex.Message));
                    exception.Data["Path"] = Path;

                    ioCommand.AffectedDataCount = 0;
                    ioCommand.Failed(exception);
                    throw exception;
                }
            }
        }

        var directory = System.IO.Path.GetDirectoryName(Path);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                var exception = new LocalFileWriteException(caller, "error while writing local file / directory creation failed", Path, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, directory creation failed, message: {1}",
                    Path, ex.Message));
                exception.Data["Path"] = Path;

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
        }

        try
        {
            var sink = caller.Context.GetSink(System.IO.Path.GetDirectoryName(Path), System.IO.Path.GetFileName(Path), sinkFormat, caller, columns);

            var stream = new FileStream(Path, FileMode, FileAccess, FileShare);
            return new NamedSink(Path, stream, ioCommand, sink);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileWriteException(caller, "error while writing local file", Path, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}",
                Path, ex.Message));
            exception.Data["Path"] = Path;

            ioCommand.AffectedDataCount = 0;
            ioCommand.Failed(exception);
            throw exception;
        }
    }
}