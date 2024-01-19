namespace FizzCode.EtLast;

public sealed class DeserializeFromJsonFileJob<T> : AbstractProcessWithResult<IEnumerable<T>>
{
    [ProcessParameterMustHaveValue]
    public required IStreamProvider StreamProvider { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public JsonSerializerOptions SerializerOptions { get; init; } = new JsonSerializerOptions()
    {
        WriteIndented = true,
    };

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<T> ExecuteImpl()
    {
        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        var streamIndex = 0;
        foreach (var stream in streams)
        {
            T result = default;

            try
            {
                using (var reader = new StreamReader(stream.Stream, Encoding))
                {
                    var content = reader.ReadToEnd();
                    result = JsonSerializer.Deserialize<T>(content, SerializerOptions);
                }

                stream.IoCommand.AffectedDataCount += 1;
                stream.IoCommand.End();
            }
            catch (Exception ex)
            {
                var exception = new JsonDeserializerException(this, "error while deserializing a json file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while deserializing a json file: {0}", stream.Name));
                exception.Data["StreamName"] = stream.Name;
                exception.Data["StreamIndex"] = streamIndex;

                stream.IoCommand.Failed(exception);
                throw exception;
            }
            finally
            {
                stream.Stream.Flush();
                stream.Stream.Close();
                stream.Stream.Dispose();
            }

            yield return result;
            streamIndex++;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeserializeFromJsonFileJobFluent
{
    public static IFlow DeserializeFromJsonFile<TResult>(this IFlow builder, out IEnumerable<TResult> result, Func<DeserializeFromJsonFileJob<TResult>> processCreator)
    {
        return builder.ExecuteProcessWithResult(out result, processCreator);
    }
}