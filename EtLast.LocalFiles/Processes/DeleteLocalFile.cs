namespace FizzCode.EtLast;

public sealed class DeleteLocalFile : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        if (!File.Exists(Path))
        {
            Context.Log(LogSeverity.Debug, this, "can't delete local file because it doesn't exist '{FileName}'", Path);
            return;
        }

        Context.Log(LogSeverity.Information, this, "deleting local file '{FileName}'", Path);

        try
        {
            File.Delete(Path);
            Context.Log(LogSeverity.Debug, this, "successfully deleted local file '{FileName}'",
                Path);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileDeleteException(this, "local file deletion failed", Path, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file deletion failed, file name: {0}, message: {1}",
                Path, ex.Message));
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DeleteLocalFileFluent
{
    public static IFlow DeleteLocalFile(this IFlow builder, Func<DeleteLocalFile> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }

    public static IFlow DeleteLocalFile(this IFlow builder, string name, string path)
    {
        return builder.ExecuteProcess(() => new DeleteLocalFile()
        {
            Name = name,
            Path = path,
        });
    }
}