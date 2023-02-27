namespace FizzCode.EtLast;

public enum LocalSinkFileExistsAction { None, Exception, Overwrite }

public class LocalFileSinkProvider : ISinkProvider
{
    /// <summary>
    /// Generates file name based on a partition key.
    /// </summary>
    public Func<string, string> FileNameGenerator { get; init; }

    /// <summary>
    /// Default value is <see cref="LocalSinkFileExistsAction.Exception"/>.
    /// </summary>
    public LocalSinkFileExistsAction ActionWhenFileExists { get; init; } = LocalSinkFileExistsAction.Exception;

    /// <summary>
    /// Default value is <see cref="FileMode.Append"/>.
    /// </summary>
    public FileMode FileMode { get; init; } = FileMode.Append;

    /// <summary>
    /// Default value is <see cref="FileAccess.Write"/>.
    /// </summary>
    public FileAccess FileAccess { get; init; } = FileAccess.Write;

    /// <summary>
    /// Default value is <see cref="= FileShare.Read"/>.
    /// </summary>
    public FileShare FileShare { get; init; } = FileShare.Read;

    public bool AutomaticallyDispose => true;

    public void Validate(IProcess caller)
    {
        if (FileNameGenerator == null)
            throw new ProcessParameterNullException(caller, "SinkProvider." + nameof(FileNameGenerator));
    }

    public NamedSink GetSink(IProcess caller, string partitionKey)
    {
        var fileName = FileNameGenerator.Invoke(partitionKey);

        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileWrite, Path.GetDirectoryName(fileName), Path.GetFileName(fileName), null, null, null, null,
            "writing to local file {FileName}", fileName);

        if (File.Exists(fileName))
        {
            if (ActionWhenFileExists == LocalSinkFileExistsAction.Exception)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", fileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    fileName));

                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, 0, exception);
                throw exception;
            }
            else if (ActionWhenFileExists == LocalSinkFileExistsAction.Overwrite)
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

                    caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, null, exception);
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

                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, null, exception);
                throw exception;
            }
        }

        try
        {
            var sinkUid = caller.Context.GetSinkUid(Path.GetDirectoryName(fileName), Path.GetFileName(fileName));

            var stream = new FileStream(fileName, FileMode, FileAccess, FileShare);
            return new NamedSink(fileName, stream, iocUid, IoCommandKind.fileWrite, sinkUid);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileWriteException(caller, "error while writing local file", fileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}",
                fileName, ex.Message));
            exception.Data["FileName"] = fileName;

            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, null, exception);
            throw exception;
        }
    }
}
