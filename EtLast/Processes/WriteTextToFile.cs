namespace FizzCode.EtLast;

public sealed class WriteTextToFile : AbstractJob
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

        var sink = SinkProvider.GetSink(this, "txt", null);
        Context.Log(LogSeverity.Information, this, "saving text file to sink {SinkName}", sink.Name);

        try
        {
            sink.Stream.Write(contentBytes);
            sink.Close();
        }
        catch (Exception ex)
        {
            var exception = new ProcessExecutionException(this, "text file write failed", ex);
            exception.Data["SinkName"] = sink.Name;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteTextToFileFluent
{
    public static IFlow WriteTextToFile(this IFlow builder, Func<WriteTextToFile> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}