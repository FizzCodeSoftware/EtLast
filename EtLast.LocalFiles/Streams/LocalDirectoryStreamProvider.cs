namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;

    public class LocalDirectoryStreamProvider : IStreamProvider
    {
        public string Path { get; init; }
        public string SearchPattern { get; init; }

        /// <summary>
        /// Default value is true.
        /// </summary>
        public bool ThrowExceptionWhenFileNotFound { get; init; } = true;

        public string Topic => Path != null
            ? PathHelpers.GetFriendlyPathName(Path)
                + (SearchPattern != null ? @"\" + SearchPattern : "")
            : null;

        public IEnumerable<NamedStream> GetStreams(IProcess caller)
        {
            var fileNames = new List<string>();

            if (Directory.Exists(Path))
            {
                if (SearchPattern != null)
                {
                    fileNames.AddRange(Directory.EnumerateFiles(Path, SearchPattern));
                }
                else
                {
                    fileNames.AddRange(Directory.EnumerateFiles(Path));
                }
            }

            if (fileNames.Count == 0)
            {
                if (ThrowExceptionWhenFileNotFound)
                {
                    var exception = new LocalFileReadException(caller, "local directory doesn't contain any matching files", Path);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local directory doesn't contain any matching files: {0}",
                        Path));

                    throw exception;
                }

                yield break;
            }

            foreach (var fileName in fileNames)
            {
                yield return GetFileStream(caller, fileName);
            }
        }

        private NamedStream GetFileStream(IProcess caller, string fileName)
        {
            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileRead, PathHelpers.GetFriendlyPathName(fileName), null, null, null, null,
                "reading from local file {FileName}", PathHelpers.GetFriendlyPathName(fileName));

            if (!File.Exists(fileName))
            {
                if (ThrowExceptionWhenFileNotFound)
                {
                    var exception = new LocalFileReadException(caller, "local file doesn't exist", fileName);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file doesn't exist: {0}",
                        fileName));

                    caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, 0, exception);
                    throw exception;
                }

                caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.fileRead, iocUid, 0);
                return null;
            }

            try
            {
                var stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                return new NamedStream(fileName, stream, iocUid, IoCommandKind.fileRead);
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

                var exception = new LocalFileReadException(caller, "error while opening local file", Path, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", fileName, ex.Message));
                exception.Data.Add("FileName", fileName);
                throw exception;
            }
        }
    }
}