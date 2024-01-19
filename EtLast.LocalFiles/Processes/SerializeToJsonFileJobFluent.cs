namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SerializeToLocalJsonFileFluent
{
    public static IFlow SerializeToLocalJsonFile<T>(this IFlow builder, string targetFileName, T data, Encoding customEncoding = null)
    {
        return builder.ExecuteProcess(() => new SerializeToJsonFileJob<T>()
        {
            Data = data,
            Encoding = customEncoding ?? Encoding.UTF8,
            SinkProvider = new LocalFileSinkProvider()
            {
                FileName = targetFileName,
                ActionWhenFileExists = LocalSinkFileExistsAction.DeleteAndContinue,
                FileMode = FileMode.CreateNew,
            },
        });
    }
}