namespace FizzCode.EtLast;

public class MemoryStreamProvider : IStreamProvider
{
    public required Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _streamName = "MemoryStream";
    private readonly string _streamLocation = "memory";
    private readonly string _streamPath = "memory";

    public string GetTopic()
    {
        return _streamName;
    }

    public void Validate(IProcess caller)
    {
        if (StreamCreator == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(StreamCreator));

        if (_streamName == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamName));

        if (_streamLocation == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamLocation));

        if (_streamPath == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamPath));
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.streamRead, _streamLocation, _streamPath, null, null, null, null,
            "reading from stream {StreamName}", _streamName);

        try
        {
            var stream = StreamCreator.Invoke();
            return new[]
            {
                new NamedStream(_streamName, stream, iocUid, IoCommandKind.streamRead),
            };
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while opening memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening memory stream: {0}, message: {1}",
                _streamName, ex.Message));
            exception.Data["StreamName"] = _streamName;

            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, exception);
            throw exception;
        }
    }
}