using System.ComponentModel;

namespace FizzCode.EtLast;

public sealed class DeleteLocalFile(IEtlContext context) : AbstractJob(context)
{
    [ProcessParameterMustHaveValue]
    public required string FileName { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        if (!File.Exists(FileName))
        {
            Context.Log(LogSeverity.Debug, this, "can't delete local file because it doesn't exist '{FileName}'", FileName);
            return;
        }

        Context.Log(LogSeverity.Information, this, "deleting local file '{FileName}'", FileName);

        try
        {
            File.Delete(FileName);
            Context.Log(LogSeverity.Debug, this, "successfully deleted local file '{FileName}'",
                FileName);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileDeleteException(this, "local file deletion failed", FileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file deletion failed, file name: {0}, message: {1}",
                FileName, ex.Message));
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DeleteLocalFileFluent
{
    public static IFlow DeleteLocalFile(this IFlow builder, Func<DeleteLocalFile> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}