namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SerializeToLocalJsonFileFluent
{
    public static IFlow SerializeToLocalJsonFile<T>(this IFlow builder, T data, string targetFileName)
    {
        return builder.ExecuteProcess(() => new SerializeToJsonFileJob<T>()
        {
            Overwrite = true,
            SinkProvider = new LocalFileSinkProvider()
            {
                FileNameGenerator = _ => targetFileName,
                ActionWhenFileExists = LocalSinkFileExistsAction.DeleteAndContinue,
                FileMode = FileMode.CreateNew,
            },
        });
    }
}