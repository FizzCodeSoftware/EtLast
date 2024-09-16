namespace FizzCode.EtLast;

public enum LocalSinkFileExistsAction { Continue, ThrowException, Overwrite }

[ContainsProcessParameterValidation]
public class PartitionedLocalFileSinkProvider : IPartitionedSinkProvider
{
    /// <summary>
    /// Generates file name based on a partition key.
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required Func<string, string> PathGenerator { get; init; }

    public SinkMetadataEnricher SinkMetadataEnricher { get; init; }

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

    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns)
    {
        var path = PathGenerator.Invoke(partitionKey);

        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileWrite,
            Location = Path.GetDirectoryName(path),
            Path = Path.GetFileName(path),
            Message = "writing to local file",
        });

        if (ActionWhenFileExists != LocalSinkFileExistsAction.Continue && File.Exists(path))
        {
            if (ActionWhenFileExists == LocalSinkFileExistsAction.ThrowException)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", path);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    path));

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
            else if (ActionWhenFileExists == LocalSinkFileExistsAction.Overwrite)
            {
                try
                {
                    File.Delete(path);
                }
                catch (Exception ex)
                {
                    var exception = new LocalFileWriteException(caller, "error while writing local file / file deletion failed", path, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, file deletion failed, message: {1}",
                        path, ex.Message));
                    exception.Data["Path"] = path;

                    ioCommand.AffectedDataCount = 0;
                    ioCommand.Failed(exception);
                    throw exception;
                }
            }
        }

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                var exception = new LocalFileWriteException(caller, "error while writing local file / directory creation failed", path, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, directory creation failed, message: {1}",
                    path, ex.Message));
                exception.Data["Path"] = path;

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }
        }

        try
        {
            var sink = caller.Context.GetSink(Path.GetDirectoryName(path), Path.GetFileName(path), sinkFormat, caller, columns);
            var stream = new FileStream(path, FileMode, FileAccess, FileShare);
            var namedSink = new NamedSink(path, stream, ioCommand, sink);
            SinkMetadataEnricher?.Enrich(namedSink.Sink);
            return namedSink;
        }
        catch (Exception ex)
        {
            var exception = new LocalFileWriteException(caller, "error while writing local file", path, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}",
                path, ex.Message));
            exception.Data["Path"] = path;

            ioCommand.AffectedDataCount = 0;
            ioCommand.Failed(exception);
            throw exception;
        }
    }
}