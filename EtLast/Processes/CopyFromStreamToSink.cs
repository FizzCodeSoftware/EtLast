namespace FizzCode.EtLast;

public sealed class CopyFromStreamToSink : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required IOneStreamProvider StreamProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required IOneSinkProvider SinkProvider { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var stream = StreamProvider.GetStream(this);
        try
        {
            var sink = SinkProvider.GetSink(this, null, null);
            try
            {
                stream.Stream.CopyTo(sink.Stream);

                sink.IoCommand.AffectedDataCount += 1;
                sink.IoCommand.End();
            }
            finally
            {
                sink.Dispose();
            }

            stream.IoCommand.AffectedDataCount += 1;
            stream.IoCommand.End();
        }
        finally
        {
            stream.Dispose();
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CopyFromStreamToSinkFluent
{
    public static IFlow CopyFromStreamToSink(this IFlow builder, Func<CopyFromStreamToSink> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}