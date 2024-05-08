namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFilesStreamProvider : IManyStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required string[] Paths { get; init; }

    public static FileStreamOptions DefaultOptions => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.Read,
        Options = FileOptions.None,
        BufferSize = 4096,
        PreallocationSize = 0,
    };

    /// <summary>
    /// Default value is <see cref="DefaultOptions"/>
    /// </summary>
    public FileStreamOptions Options { get; init; } = DefaultOptions;

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool ThrowExceptionWhenFileNotFound { get; init; } = true;

    public string GetTopic()
    {
        return Paths?.Length > 0
            ? PathHelpers.GetFriendlyPathName(Paths[0]) + "+" + Paths.Length.ToString("D", CultureInfo.InvariantCulture) + ""
            : null;
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        foreach (var path in Paths)
        {
            yield return GetFileStream(caller, path);
        }
    }

    private NamedStream GetFileStream(IProcess caller, string path)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileRead,
            Location = Path.GetDirectoryName(path),
            Path = Path.GetFileName(path),
            Message = "reading from local file",
        });

        if (!File.Exists(path))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist");
                exception.Data["Path"] = path;
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    path));

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }

            ioCommand.AffectedDataCount = 0;
            ioCommand.End();
            return null;
        }

        try
        {
            var stream = new FileStream(path, Options);
            return new NamedStream(path, stream, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", ex.Message));
            exception.Data["Path"] = path;

            ioCommand.AffectedDataCount = 0;
            ioCommand.Failed(exception);
            throw exception;
        }
    }
}
