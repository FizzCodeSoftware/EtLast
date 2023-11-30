namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class LocalFileSetStreamProvider : IStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required string[] FileNames { get; init; }

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
        return FileNames?.Length > 0
            ? PathHelpers.GetFriendlyPathName(FileNames[0]) + "+" + FileNames.Length.ToString("D", CultureInfo.InvariantCulture) + ""
            : null;
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        foreach (var fileName in FileNames)
        {
            yield return GetFileStream(caller, fileName);
        }
    }

    private NamedStream GetFileStream(IProcess caller, string fileName)
    {
        var ioCommand = caller.Context.RegisterIoCommandStart(caller, new IoCommand()
        {
            Kind = IoCommandKind.fileRead,
            Location = Path.GetDirectoryName(fileName),
            Path = Path.GetFileName(fileName),
            Message = "reading from local file",
        });

        if (!File.Exists(fileName))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist", fileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    fileName));

                ioCommand.Exception = exception;
                ioCommand.AffectedDataCount = 0;
                caller.Context.RegisterIoCommandEnd(caller, ioCommand);
                throw exception;
            }

            ioCommand.AffectedDataCount = 0;
            caller.Context.RegisterIoCommandEnd(caller, ioCommand);
            return null;
        }

        try
        {
            var stream = new FileStream(fileName, Options);
            return new NamedStream(fileName, stream, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", fileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", fileName, ex.Message));
            exception.Data["FileName"] = fileName;

            ioCommand.Exception = exception;
            ioCommand.AffectedDataCount = 0;
            caller.Context.RegisterIoCommandEnd(caller, ioCommand);
            throw exception;
        }
    }
}
