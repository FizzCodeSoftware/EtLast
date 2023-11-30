namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileStreamProvider : IStreamProvider
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

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommandStart(caller, new IoCommand()
        {
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

                ioCommand.Exception = exception;
                ioCommand.AffectedDataCount = 0;
                caller.Context.RegisterIoCommandEnd(caller, ioCommand);
                throw exception;
            }

            ioCommand.AffectedDataCount = 0;
            caller.Context.RegisterIoCommandEnd(caller, ioCommand);
            return Enumerable.Empty<NamedStream>();
        }

        try
        {
            var stream = new FileStream(FileName, Options);
            return new[]
            {
                new NamedStream(FileName, stream, ioCommand),
            };
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", FileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}",
                FileName, ex.Message));
            exception.Data["FileName"] = FileName;

            ioCommand.Exception = exception;
            ioCommand.AffectedDataCount = 0;
            caller.Context.RegisterIoCommandEnd(caller, ioCommand);
            throw exception;
        }
    }
}
