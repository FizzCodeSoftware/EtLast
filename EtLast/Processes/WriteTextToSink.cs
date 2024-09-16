namespace FizzCode.EtLast;

public sealed class WriteTextToSink : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required IOneSinkProvider SinkProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public Func<string> ContentGenerator { get; init; }

    /// <summary>
    /// Default value is <see cref="Encoding.UTF8"/>
    /// </summary>
    public Encoding Encoding { get; init; } = Encoding.UTF8;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        if (ContentGenerator == null)
            return;

        var content = ContentGenerator.Invoke();
        var contentBytes = content != null
            ? Encoding.GetBytes(content)
            : [];

        var namedSink = SinkProvider.GetSink(this, "txt", null);
        Context.Log(LogSeverity.Information, this, "saving text file to sink {SinkName}", namedSink.Name);

        try
        {
            if (contentBytes.Length > 0)
            {
                namedSink.Stream.Write(contentBytes);
                namedSink.Sink.IncreaseRows(1);
                namedSink.Sink.IncreaseBytes(contentBytes.Length);
                namedSink.Sink.IncreaseCharacters(content?.Length ?? 0);
            }

            namedSink.Close();
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, "text file write failed", ex);
            exception.Data["SinkName"] = namedSink.Name;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteTextToSinkFluent
{
    public static IFlow WriteTextToSink(this IFlow builder, Func<WriteTextToSink> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}