namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class MemoryStreamProvider : IStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _streamName = "MemoryStream";

    public string GetTopic()
    {
        return _streamName;
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var ioCommandId = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.streamRead, null, null, null, null, null,
            "reading from memory stream");

        try
        {
            var stream = StreamCreator.Invoke();
            return new[]
            {
                new NamedStream(_streamName, stream, ioCommandId, IoCommandKind.streamRead),
            };
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while opening memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening memory stream: {0}, message: {1}",
                _streamName, ex.Message));
            exception.Data["StreamName"] = _streamName;

            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, ioCommandId, null, exception);
            throw exception;
        }
    }
}