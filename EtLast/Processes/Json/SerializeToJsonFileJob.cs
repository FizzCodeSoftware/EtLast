namespace FizzCode.EtLast;

public sealed class SerializeToJsonFileJob<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required ISinkProvider SinkProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public T Data { get; init; }

    public JsonTypeInfo CustomJsonTypeInfo { get; init; }

    public required bool Overwrite { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            var content = CustomJsonTypeInfo == null
                ? JsonSerializer.Serialize(Data)
                : JsonSerializer.Serialize(Data, CustomJsonTypeInfo);

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