namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class PartitionedMemorySinkProvider : IOneSinkProvider, IPartitionedSinkProvider
{
    [ProcessParameterMustHaveValue]
    public required Func<string, MemoryStream> StreamCreator { get; init; }

    public SinkMetadataEnricher SinkMetadataEnricher { get; init; }

    private readonly string _sinkLocation = "memory";

    /// <summary>
    /// Default value is false
    /// </summary>
    public required bool AutomaticallyDispose { get; init; }

    public NamedSink GetSink(IProcess caller, string sinkFormat, string[] columns)
    {
        return GetSink(caller, null, sinkFormat, columns);
    }

    public NamedSink GetSink(IProcess caller, string partitionKey, string sinkFormat, string[] columns)
    {
        var name = Guid.CreateVersion7().ToString("N");

        try
        {
            var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
            {
                Process = caller,
                Kind = IoCommandKind.memoryWrite,
                Location = _sinkLocation,
                Path = name,
                Message = "writing to memory stream",
            });

            var stream = StreamCreator.Invoke(partitionKey);

            var sink = caller.Context.GetSink(_sinkLocation, name, sinkFormat, caller, columns);
            var namedSink = new NamedSink(name, stream, ioCommand, sink);
            SinkMetadataEnricher?.Enrich(namedSink.Sink);
            return namedSink;
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while creating memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while creating memory stream: message: {0}",
                ex.Message));

            throw exception;
        }
    }
}