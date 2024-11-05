namespace FizzCode.EtLast;

public sealed class ExtractZipStreamToLocalDirectory : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required IOneStreamProvider StreamProvider { get; init; }

    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    public bool OverwriteFiles { get; init; } = true;

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var stream = StreamProvider.GetStream(this);
        var startPos = 0L;
        try
        {
            startPos = stream.Stream.Position;
        }
        catch (Exception) { }

        try
        {
            if (!Directory.Exists(Path))
                Directory.CreateDirectory(Path);

            try
            {
                using (var zipArchive = new ZipArchive(stream.Stream, ZipArchiveMode.Read, true))
                {
                    zipArchive.ExtractToDirectory(Path, OverwriteFiles);
                }
            }
            finally
            {
                var endPos = 0L;
                try
                {
                    endPos = stream.Stream.Position;
                }
                catch (Exception) { }

                stream.IoCommand.AffectedDataCount += endPos - startPos;
                stream.IoCommand.End();
                stream.Close();
            }
        }
        catch (Exception ex)
        {
            var exception = new ExtractZipStreamToLocalDirectoryException(this, ex);
            exception.Data["StreamName"] = stream.Name;
            exception.Data["Path"] = Path;
            exception.Data["OverwriteFiles"] = OverwriteFiles;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ExtractZipStreamToLocalDirectoryFluent
{
    public static IFlow ExtractZipStreamToLocalDirectory(this IFlow builder, Func<ExtractZipStreamToLocalDirectory> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}