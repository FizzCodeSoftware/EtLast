namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileStreamProvider : IManyStreamProvider, IOneStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required string FileName { get; init; }

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
        return FileName != null
            ? PathHelpers.GetFriendlyPathName(FileName)
            : null;
    }

    public NamedStream GetStream(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileRead,
            Location = Path.GetDirectoryName(FileName),
            Path = Path.GetFileName(FileName),
            Message = "reading from local file",
        });

        if (!File.Exists(FileName))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    FileName));

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
            var stream = new FileStream(FileName, Options);
            return new NamedStream(FileName, stream, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", FileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}",
                FileName, ex.Message));
            exception.Data["FileName"] = FileName;

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