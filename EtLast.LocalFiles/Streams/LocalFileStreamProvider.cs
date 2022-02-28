namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

    public class LocalFileStreamProvider : IStreamProvider
    {
        public string FileName { get; init; }

        /// <summary>
        /// Default value is true.
        /// </summary>
        public bool ThrowExceptionWhenFileNotFound { get; init; } = true;

        public string Topic => FileName != null ? PathHelpers.GetFriendlyPathName(FileName) : null;

        public NamedStream GetStream(IProcess caller)
        {
            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileRead, PathHelpers.GetFriendlyPathName(FileName), null, null, null, null,
                "reading from local file {FileName}", PathHelpers.GetFriendlyPathName(FileName));

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
                return null;
            }

            try
            {
                var stream = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                return new NamedStream(FileName, stream, iocUid, IoCommandKind.fileRead);
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

                var exception = new LocalFileReadException(caller, "error while opening local file", FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }
    }
}