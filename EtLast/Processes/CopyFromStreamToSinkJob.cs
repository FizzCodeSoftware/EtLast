namespace FizzCode.EtLast;

public sealed class CopyFromStreamToSinkJob : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required IOneStreamProvider StreamProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required IOneSinkProvider SinkProvider { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var stream = StreamProvider.GetStream(this);
        var sink = SinkProvider.GetSink(this, null, null);
        stream.Stream.CopyTo(sink.Stream);
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CopyFromStreamToSinkJobFluent
{
    public static IFlow CopyFromStreamToSink(this IFlow builder, Func<CopyFromStreamToSinkJob> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}