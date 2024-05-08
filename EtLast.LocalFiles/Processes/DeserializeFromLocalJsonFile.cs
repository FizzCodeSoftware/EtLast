namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeserializeFromLocalJsonFileFluent
{
    public static IFlow DeserializeFromLocalJsonFile<TResult>(this IFlow builder, out TResult result, string name, string path)
    {
        return builder.ExecuteProcessWithResult(out result, () => new DeserializeFromJsonStream<TResult>()
        {
            Name = name,
            StreamProvider = new LocalFileStreamProvider()
            {
                Path = path,
            },
        });
    }
}