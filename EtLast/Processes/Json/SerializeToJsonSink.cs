namespace FizzCode.EtLast;

public sealed class SerializeToJsonSink<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required IOneSinkProvider SinkProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required T Data { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public JsonSerializerOptions SerializerOptions { get; init; } = new JsonSerializerOptions()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var namedSink = SinkProvider.GetSink(this, "json", null);
        try
        {
            var content = JsonSerializer.Serialize(Data, SerializerOptions);
            var contentBytes = Encoding.GetBytes(content);
            namedSink.Stream.Write(contentBytes);

            namedSink.Sink.IncreaseRows(1);
            namedSink.Sink.IncreaseBytes(contentBytes.Length);
            namedSink.Sink.IncreaseCharacters(content.Length);

            namedSink.IoCommand.AffectedDataCount += 1;
            namedSink.IoCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new JsonSerializerException(this, "error while serializing into a json file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while serializing into a json file: {0}", namedSink.Name));
            exception.Data["StreamName"] = namedSink.Name;

            namedSink.IoCommand.Failed(exception);
            throw exception;
        }
        finally
        {
            namedSink.Stream.Flush();
            namedSink.Stream.Close();
            namedSink.Stream.Dispose();
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SerializeToJsonSinkFluent
{
    public static IFlow SerializeToJsonSink<T>(this IFlow builder, Func<SerializeToJsonSink<T>> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}