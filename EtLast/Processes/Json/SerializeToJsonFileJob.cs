namespace FizzCode.EtLast;

public sealed class SerializeToJsonFileJob<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required ISinkProvider SinkProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public T Data { get; init; }

    public required bool Overwrite { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public JsonSerializerOptions SerializerOptions { get; init; } = new JsonSerializerOptions()
    {
        WriteIndented = true,
    };

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            var content = JsonSerializer.Serialize(Data, SerializerOptions);

            var namedSink = SinkProvider.GetSink(this, null, "json", []);
            namedSink.Stream.Write(Encoding.GetBytes(content));
            namedSink.IncreaseRowsWritten();
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, ex);
            exception.Data["Overwrite"] = Overwrite;
            throw exception;
        }
    }
}