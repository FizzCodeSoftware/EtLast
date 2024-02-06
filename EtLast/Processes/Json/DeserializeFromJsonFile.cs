namespace FizzCode.EtLast;

public sealed class DeserializeFromJsonFile<T> : AbstractProcessWithResult<T>
{
    [ProcessParameterMustHaveValue]
    public required IOneStreamProvider StreamProvider { get; init; }

    public Encoding Encoding { get; init; } = Encoding.UTF8;

    public JsonSerializerOptions SerializerOptions { get; init; } = new JsonSerializerOptions()
    {
        WriteIndented = true,
    };

    protected override T ExecuteImpl()
    {
        var stream = StreamProvider.GetStream(this);
        if (stream == null)
            return default;

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

            stream.IoCommand.Failed(exception);
            throw exception;
        }
        finally
        {
            stream.Stream.Close();
            stream.Stream.Dispose();
        }

        return result;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeserializeFromJsonFileFluent
{
    public static IFlow DeserializeFromJsonFile<TResult>(this IFlow builder, out TResult result, Func<DeserializeFromJsonFile<TResult>> processCreator)
    {
        return builder.ExecuteProcessWithResult(out result, processCreator);
    }
}