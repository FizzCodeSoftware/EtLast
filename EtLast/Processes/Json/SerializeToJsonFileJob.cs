namespace FizzCode.EtLast;

public sealed class SerializeToJsonFileJob<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required ISinkProvider SinkProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public T Data { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public JsonSerializerOptions SerializerOptions { get; init; } = new JsonSerializerOptions()
    {
        WriteIndented = true,
    };

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var namedSink = SinkProvider.GetSink(this, null, "json", []);
        try
        {
            var content = JsonSerializer.Serialize(Data, SerializerOptions);

            namedSink.Stream.Write(Encoding.GetBytes(content));
            namedSink.IncreaseRowsWritten();
        }
        finally
        {
            namedSink.Stream.Flush();
            namedSink.Stream.Close();
            namedSink.Stream.Dispose();
        }
    }
}