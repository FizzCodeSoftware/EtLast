namespace FizzCode.EtLast;

public class MemorySinkProvider : ISinkProvider
{
    public Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _sinkName = "MemorySink";
    private readonly string _sinkLocation = "memory";
    private readonly string _sinkPath = "memory";

    /// <summary>
    /// Default value is false
    /// </summary>
    public bool AutomaticallyDispose { get; init; }

    public void Validate(IProcess caller)
    {
        if (StreamCreator == null)
            throw new ProcessParameterNullException(caller, "SinkProvider." + nameof(StreamCreator));

        if (_sinkName == null)
            throw new ProcessParameterNullException(caller, "SinkProvider." + nameof(_sinkName));

        if (_sinkLocation == null)
            throw new ProcessParameterNullException(caller, "SinkProvider." + nameof(_sinkLocation));

        if (_sinkPath == null)
            throw new ProcessParameterNullException(caller, "SinkProvider." + nameof(_sinkPath));
    }

    public NamedSink GetSink(IProcess caller, string partitionKey)
    {
        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.memoryWrite, _sinkLocation, _sinkPath, null, null, null, null,
            "writing to memory stream");

        try
        {
            var sinkUid = caller.Context.GetSinkUid(_sinkLocation, _sinkPath);

            var stream = StreamCreator.Invoke();
            return new NamedSink(_sinkName, stream, iocUid, IoCommandKind.streamWrite, sinkUid);
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while writing memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing memory stream: {0}, message: {1}",
                _sinkName, ex.Message));
            exception.Data["SinkName"] = _sinkName;

            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, exception);
            throw exception;
        }
    }
}