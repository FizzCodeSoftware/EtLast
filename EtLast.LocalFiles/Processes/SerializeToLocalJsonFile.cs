namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SerializeToLocalJsonFileFluent
{
    public static IFlow SerializeToLocalJsonFile<T>(this IFlow builder, string name, string path, T data)
    {
        return builder.ExecuteProcess(() => new SerializeToJsonSink<T>()
        {
            Name = name,
            Data = data,
            SinkProvider = new LocalFileSinkProvider()
            {
                Path = path,
                ActionWhenFileExists = LocalSinkFileExistsAction.Overwrite,
                FileMode = FileMode.CreateNew,
            },
        });
    }
}