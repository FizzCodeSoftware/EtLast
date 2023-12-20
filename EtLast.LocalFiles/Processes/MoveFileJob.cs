namespace FizzCode.EtLast;

public sealed class MoveFileJob : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required string SourceFileName { get; init; }

    [ProcessParameterMustHaveValue]
    public required string TargetFileName { get; init; }

    public required bool Overwrite { get; init; }

    public override string GetTopic() => SourceFileName;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            if (File.Exists(TargetFileName))
            {
                if (!Overwrite)
                    return;
            }

            File.Move(SourceFileName, TargetFileName, true);
        }
        catch (Exception ex)
        {
            var exception = new MoveFileException(this, ex);
            exception.Data["SourceFileName"] = SourceFileName;
            exception.Data["TargetFileName"] = TargetFileName;
            exception.Data["Overwrite"] = Overwrite;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MoveFileJobFluent
{
    public static IFlow MoveFile(this IFlow builder, string sourceFileName, string targetFileName, bool overwrite)
    {
        return builder.ExecuteProcess(() => new MoveFileJob()
        {
            SourceFileName = sourceFileName,
            TargetFileName = targetFileName,
            Overwrite = overwrite,
        });
    }
}