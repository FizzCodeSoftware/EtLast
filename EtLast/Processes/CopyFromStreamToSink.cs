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
        var startPos = 0L;
        var endPos = 0L;
        try
        {
            startPos = stream.Stream.Position;
        }
        catch (Exception) { }

        try
        {
            var sink = SinkProvider.GetSink(this, null, null);
            try
            {
                stream.Stream.CopyTo(sink.Stream);

                try
                {
                    endPos = stream.Stream.Position;
                }
                catch (Exception) { }

                sink.IoCommand.AffectedDataCount += endPos - startPos;
                sink.IoCommand.End();
            }
            finally
            {
                sink.Close();
            }

            stream.IoCommand.AffectedDataCount += endPos - startPos;
            stream.IoCommand.End();
        }
        finally
        {
            stream.Close();
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