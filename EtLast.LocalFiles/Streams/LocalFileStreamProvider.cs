namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

public class LocalFileStreamProvider : IStreamProvider
{
    public string FileName { get; init; }

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool ThrowExceptionWhenFileNotFound { get; init; } = true;

    public string GetTopic()
    {
        return FileName != null
            ? PathHelpers.GetFriendlyPathName(FileName)
            : null;
    }

    public void Validate(IProcess caller)
    {
        if (FileName == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(FileName));
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileRead, Path.GetDirectoryName(FileName), Path.GetFileName(FileName), null, null, null, null,
            "reading from local file {FileName}", FileName);

        if (!File.Exists(FileName))
        {
            if (ThrowExceptionWhenFileNotFound)
            {
                var exception = new LocalFileReadException(caller, "local file doesn't exist", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                    FileName));

                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, 0, exception);
                throw exception;
            }

            caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.fileRead, iocUid, 0);
            return Enumerable.Empty<NamedStream>();
        }

        try
        {
            var stream = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.Read);
            return new[]
            {
                new NamedStream(FileName, stream, iocUid, IoCommandKind.fileRead),
            };
        }
        catch (Exception ex)
        {
            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

            var exception = new LocalFileReadException(caller, "error while opening local file", FileName, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", FileName, ex.Message));
            throw exception;
        }
    }
}
