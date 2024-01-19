namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileSinkProvider : IOneSinkProvider
{
    /// <summary>
    /// Generates file name.
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required string FileName { get; init; }

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

    public NamedSink GetSink(IProcess caller, string sinkFormat, string[] columns)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileWrite,
            Location = Path.GetDirectoryName(FileName),
            Path = Path.GetFileName(FileName),
            Message = "writing to local file",
        });

        if (ActionWhenFileExists != LocalSinkFileExistsAction.Continue && File.Exists(FileName))
        {
            if (ActionWhenFileExists == LocalSinkFileExistsAction.Exception)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    FileName));

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
            else if (ActionWhenFileExists == LocalSinkFileExistsAction.DeleteAndContinue)
            {
                try
                {
                    File.Delete(FileName);
                }
                catch (Exception ex)
                {
                    var exception = new LocalFileWriteException(caller, "error while writing local file / file deletion failed", FileName, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, file deletion failed, message: {1}",
                        FileName, ex.Message));
                    exception.Data["FileName"] = FileName;

                    ioCommand.AffectedDataCount = 0;
                    ioCommand.Failed(exception);
                    throw exception;
                }
            }
        }

        var directory = Path.GetDirectoryName(FileName);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                var exception = new LocalFileWriteException(caller, "error while writing local file / directory creation failed", FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, directory creation failed, message: {1}",
                    FileName, ex.Message));
                exception.Data["FileName"] = FileName;
                exception.Data["Directory"] = directory;

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
        }

        try
        {
            var sink = caller.Context.GetSink(Path.GetDirectoryName(FileName), Path.GetFileName(FileName), sinkFormat, caller, columns);

            var stream = new FileStream(FileName, FileMode, FileAccess, FileShare);
            return new NamedSink(FileName, stream, ioCommand, sink);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileWriteException(caller, "error while writing local file", FileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}",
                FileName, ex.Message));
            exception.Data["FileName"] = FileName;

            ioCommand.AffectedDataCount = 0;
            ioCommand.Failed(exception);
            throw exception;
        }
    }
}