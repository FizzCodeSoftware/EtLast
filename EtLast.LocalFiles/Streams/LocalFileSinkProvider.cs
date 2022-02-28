namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

    public class LocalFileSinkProvider : ISinkProvider
    {
        public string FileName { get; init; }

        /// <summary>
        /// Default value is true.
        /// </summary>
        public bool ThrowExceptionWhenFileExists { get; init; } = true;

        public string Topic => FileName != null ? PathHelpers.GetFriendlyPathName(FileName) : null;

        public bool AutomaticallyDispose => true;

        public NamedSink GetSink(IProcess caller, string partitionKey)
        {
            var fileName = FileName;
            if (partitionKey != null)
            {
                fileName = string.Format(FileName, partitionKey);
            }

            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileWrite, PathHelpers.GetFriendlyPathName(fileName), null, null, null, null,
                "writing to local file {FileName}", PathHelpers.GetFriendlyPathName(fileName));

            if (File.Exists(fileName) && ThrowExceptionWhenFileExists)
            {
                var exception = new LocalFileWriteException(caller, "local file already exist", fileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file already exist: {0}",
                    fileName));

                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, 0, exception);
                throw exception;
            }

            try
            {
                var sinkUid = caller.Context.GetSinkUid(Path.GetDirectoryName(fileName), Path.GetFileName(fileName));

                var stream = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.Read);
                return new NamedSink(fileName, stream, iocUid, IoCommandKind.fileWrite, sinkUid);
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileWrite, iocUid, null, ex);

                var exception = new LocalFileWriteException(caller, "error while writing local file", fileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing local file: {0}, message: {1}", fileName, ex.Message));
                throw exception;
            }
        }
    }
}