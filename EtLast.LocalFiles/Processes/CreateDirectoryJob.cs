namespace FizzCode.EtLast;

public sealed class CreateDirectoryJob : AbstractJob
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

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CreateDirectoryJobFluent
{
    public static IFlow CreateDirectory(this IFlow builder, string path)
    {
        return builder.ExecuteProcess(() => new CreateDirectoryJob()
        {
            Path = path,
        });
    }
}