namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class MultipleLocalFilesInDirectoryStreamProvider : IManyStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required string Directory { get; init; }

    /// <summary>
    /// Default value is "*.*"
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required string SearchPattern { get; init; } = "*.*";

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool ThrowExceptionWhenFileNotFound { get; init; } = true;

    public string GetTopic()
    {
        return Directory != null
            ? PathHelpers.GetFriendlyPathName(Directory)
                + (SearchPattern != null ? @"\" + SearchPattern : "")
            : null;
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var paths = new List<string>();

        if (System.IO.Directory.Exists(Directory))
            paths.AddRange(System.IO.Directory.EnumerateFiles(Directory, SearchPattern));

        if (paths.Count == 0)
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local directory doesn't contain any matching files");
                exception.Data["Path"] = Directory;
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local directory doesn't contain any matching files: {0}",
                    Directory));

                throw exception;
            }

            yield break;
        }

        foreach (var path in paths)
            yield return GetFileStream(caller, path);
    }

    private NamedStream GetFileStream(IProcess caller, string path)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.fileRead,
            Location = Directory,
            Path = path.Replace(Directory, "", StringComparison.InvariantCultureIgnoreCase),
            Message = "reading from local file",
        });

        if (!File.Exists(path))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist");
                exception.Data["Path"] = path;
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    path));

                ioCommand.AffectedDataCount = 0;
                ioCommand.Failed(exception);
                throw exception;
            }

            ioCommand.AffectedDataCount = 0;
            ioCommand.End();
            return null;
        }

        try
        {
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            return new NamedStream(path, stream, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new LocalFileReadException(caller, "error while opening local file", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", path, ex.Message));
            exception.Data["Path"] = path;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}
