using System.Text.Json;
using System.Threading.Tasks;

namespace FizzCode.EtLast;

public sealed class JsonArrayReader<T> : AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required IStreamProvider StreamProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required string ColumnName { get; init; }

    /// <summary>
    /// First stream index is (integer) 0
    /// </summary>
    public string AddStreamIndexToColumn { get; init; }

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

        var streamIndex = 0;
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
                var entryIndex = 0;
                var enumerator = JsonSerializer.DeserializeAsyncEnumerable<T>(stream.Stream, cancellationToken: Context.CancellationToken).ToBlockingEnumerable().GetEnumerator();
                while (!FlowState.IsTerminating)
                {
                    try
                    {
                        var finished = !enumerator.MoveNext();
                        if (finished)
                            break;
                    }
                    catch (Exception ex)
                    {
                        var exception = new JsonArrayReaderException(this, "json input contains one or more errors", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "json input contains one or more errors: {0}", stream.Name));
                        exception.Data["StreamName"] = stream.Name;
                        exception.Data["StreamIndex"] = streamIndex;
                        exception.Data["EntryIndex"] = entryIndex;
                        exception.Data["ResultCount"] = resultCount;

                        stream.IoCommand.Failed(exception);
                        throw exception;
                    }

                    var entry = enumerator.Current;

                    resultCount++;
                    initialValues[ColumnName] = entry;

                    if (!string.IsNullOrEmpty(AddStreamIndexToColumn))
                        initialValues[AddStreamIndexToColumn] = streamIndex;

                    yield return Context.CreateRow(this, initialValues);
                }
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

            streamIndex++;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
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