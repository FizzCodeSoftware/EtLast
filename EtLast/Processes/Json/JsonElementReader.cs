namespace FizzCode.EtLast;

public sealed class JsonElementReader<T> : AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required IStreamProvider StreamProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required string ColumnName { get; init; }

    public override string GetTopic()
    {
        return StreamProvider?.GetTopic();
    }

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> Produce()
    {
        var resultCount = 0;

        var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (FlowState.IsTerminating)
                break;

            var startPos = 0L;
            try
            {
                startPos = stream.Stream.Position;
            }
            catch (Exception) { }

            try
            {
                var entry = JsonSerializer.Deserialize<T>(stream.Stream);
                resultCount++;
                initialValues[ColumnName] = entry;

                yield return Context.CreateRow(this, initialValues);
            }
            finally
            {
                if (stream != null)
                {
                    var endPos = 0L;
                    try
                    {
                        endPos = stream.Stream.Position;
                    }
                    catch (Exception) { }

                    stream.IoCommand.AffectedDataCount += endPos - startPos;
                    stream.IoCommand.End();
                    stream.Dispose();
                }
            }

            if (FlowState.IsTerminating)
                break;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class JsonOneReaderFluent
{
    /// <summary>
    /// Read one single, strongly typed element from the input stream(s).
    /// If multiple streams are resolved, one element will be read from each.
    /// </summary>
    public static IFluentSequenceMutatorBuilder ReadJsonElement<T>(this IFluentSequenceBuilder builder, JsonElementReader<T> reader)
    {
        return builder.ReadFrom(reader);
    }
}