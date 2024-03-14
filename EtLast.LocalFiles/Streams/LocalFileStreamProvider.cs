namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileStreamProvider : IManyStreamProvider, IOneStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

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
        return Path != null
            ? PathHelpers.GetFriendlyPathName(Path)
            : null;
    }

    public NamedStream GetStream(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileRead,
            Location = System.IO.Path.GetDirectoryName(Path),
            Path = System.IO.Path.GetFileName(Path),
            Message = "reading from local file",
        });

        if (!File.Exists(Path))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist");
                exception.Data["Path"] = Path;
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    Path));

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
            var stream = new FileStream(Path, Options);
            return new NamedStream(Path, stream, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", ex);
            exception.Data["Path"] = Path;
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}",
                Path, ex.Message));

            ioCommand.AffectedDataCount = 0;
            ioCommand.Failed(exception);
            throw exception;
        }
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var stream = GetStream(caller);
        return new[] { stream };
    }
}