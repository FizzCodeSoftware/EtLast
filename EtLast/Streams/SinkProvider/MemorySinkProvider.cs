namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class MemorySinkProvider : ISinkProvider
{
    [ProcessParameterMustHaveValue]
    public required Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _sinkName = "MemorySink";
    private readonly string _sinkLocation = "memory";
    private readonly string _sinkPath = "memory";

    /// <summary>
    /// Default value is false
    /// </summary>
    public required bool AutomaticallyDispose { get; init; }

    public NamedSink GetSink(IProcess caller, string partitionKey)
    {
        var ioCommandId = caller.Context.RegisterIoCommandStartWithPath(caller, IoCommandKind.memoryWrite, _sinkLocation, _sinkPath, null, null, null, null,
            "writing to memory stream", null);

        try
        {
            var sinkId = caller.Context.GetSinkId(_sinkLocation, _sinkPath);

            var stream = StreamCreator.Invoke();
            return new NamedSink(_sinkName, stream, ioCommandId, IoCommandKind.streamWrite, sinkId);
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while writing memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing memory stream: {0}, message: {1}",
                _sinkName, ex.Message));
            exception.Data["SinkName"] = _sinkName;

            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, ioCommandId, null, exception);
            throw exception;
        }
    }
}