using System.Text.Json;
using System.Threading.Tasks;

namespace FizzCode.EtLast;

public sealed class JsonArrayReader<T> : AbstractRowSource
{
    public required IStreamProvider StreamProvider { get; init; }
    public required string ColumnName { get; init; }

    /// <summary>
    /// First stream index is (integer) 0
    /// </summary>
    public string AddStreamIndexToColumn { get; init; }

    public JsonArrayReader(IEtlContext context)
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

        var streamIndex = 0;
        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (FlowState.IsTerminating)
                break;

            try
            {
                var enumerable = JsonSerializer.DeserializeAsyncEnumerable<T>(stream.Stream, cancellationToken: Context.CancellationToken).ToBlockingEnumerable();
                foreach (var entry in enumerable)
                {
                    resultCount++;
                    initialValues[ColumnName] = entry;

                    if (!string.IsNullOrEmpty(AddStreamIndexToColumn))
                        initialValues[AddStreamIndexToColumn] = streamIndex;

                    yield return Context.CreateRow(this, initialValues);

                    if (FlowState.IsTerminating)
                        break;
                }
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

            streamIndex++;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class JsonArrayReaderFluent
{
    /// <summary>
    /// Read an array of strongly typed elements from the input stream(s).
    /// </summary>
    public static IFluentSequenceMutatorBuilder ReadJsonArray<T>(this IFluentSequenceBuilder builder, JsonArrayReader<T> reader)
    {
        return builder.ReadFrom(reader);
    }
}