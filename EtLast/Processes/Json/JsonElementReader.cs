using System.Text.Json;

namespace FizzCode.EtLast;

public sealed class JsonElementReader<T> : AbstractRowSource
{
    public required IStreamProvider StreamProvider { get; init; }
    public required string ColumnName { get; init; }

    public JsonElementReader(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return StreamProvider?.GetTopic();
    }

    protected override void ValidateImpl()
    {
        if (StreamProvider == null)
            throw new ProcessParameterNullException(this, nameof(StreamProvider));

        if (ColumnName == null)
            throw new ProcessParameterNullException(this, nameof(ColumnName));

        StreamProvider.Validate(this);
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
                    Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandUid, resultCount);
                    stream.Dispose();
                }
            }

            if (FlowState.IsTerminating)
                break;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
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