namespace FizzCode.EtLast;

public sealed class DeleteLocalDirectory : AbstractJob
{
    [ProcessParameterMustHaveValue] public required string Path { get; init; }
    public required bool Recursive { get; init; }

    public override string GetTopic() => Path;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            if (Directory.Exists(Path))
                Directory.Delete(Path, Recursive);
        }
        catch (Exception ex)
        {
            var exception = new DeleteDirectoryException(this, ex);
            exception.Data["Path"] = Path;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeleteLocalDirectoryFluent
{
    public static IFlow DeleteLocalDirectory(this IFlow builder, string path, bool recursive)
    {
        return builder.ExecuteProcess(() => new DeleteLocalDirectory()
        {
            Path = path,
            Recursive = recursive,
        });
    }
}