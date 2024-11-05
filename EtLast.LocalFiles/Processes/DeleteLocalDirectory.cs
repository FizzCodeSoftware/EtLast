namespace FizzCode.EtLast;

public sealed class DeleteLocalDirectory : AbstractJob
{
    [ProcessParameterMustHaveValue] public required string Path { get; init; }
    public required bool Recursive { get; init; }

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
            exception.Data["Recursive"] = Recursive;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeleteLocalDirectoryFluent
{
    public static IFlow DeleteLocalDirectory(this IFlow builder, Func<DeleteLocalDirectory> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }

    public static IFlow DeleteLocalDirectory(this IFlow builder, string name, string path, bool recursive)
    {
        return builder.ExecuteProcess(() => new DeleteLocalDirectory()
        {
            Name = name,
            Path = path,
            Recursive = recursive,
        });
    }
}