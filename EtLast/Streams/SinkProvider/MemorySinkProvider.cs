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

    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.memoryWrite,
            Location = _sinkLocation,
            Path = _sinkPath,
            Message = "writing to memory stream",
        });

        try
        {
            var sink = caller.Context.GetSink(_sinkLocation, _sinkPath, sinkFormat, caller, columns);

            var stream = StreamCreator.Invoke();
            return new NamedSink(_sinkName, stream, ioCommand, sink);
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while writing memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing memory stream: {0}, message: {1}",
                _sinkName, ex.Message));

            exception.Data["SinkName"] = _sinkName;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}