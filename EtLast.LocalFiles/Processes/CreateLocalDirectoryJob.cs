namespace FizzCode.EtLast;

public sealed class CreateLocalDirectoryJob : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    public override string GetTopic() => Path;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            if (!Directory.Exists(Path))
                Directory.CreateDirectory(Path);
        }
        catch (Exception ex)
        {
            var exception = new CreateDirectoryException(this, ex);
            exception.Data["Path"] = Path;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CreateLocalDirectoryJobFluent
{
    public static IFlow CreateLocalDirectory(this IFlow builder, string path)
    {
        return builder.ExecuteProcess(() => new CreateLocalDirectoryJob()
        {
            Path = path,
        });
    }
}