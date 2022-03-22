namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;

    public class LocalDirectoryStreamProvider : IStreamProvider
    {
        public string Directory { get; init; }

        /// <summary>
        /// Default value is "*.*"
        /// </summary>
        public string SearchPattern { get; init; } = "*.*";

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

        public void Validate(IProcess caller)
        {
            if (Directory == null)
                throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(Directory));

            if (SearchPattern == null)
                throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(SearchPattern));
        }

        public IEnumerable<NamedStream> GetStreams(IProcess caller)
        {
            var fileNames = new List<string>();

            if (System.IO.Directory.Exists(Directory))
            {
                fileNames.AddRange(System.IO.Directory.EnumerateFiles(Directory, SearchPattern));
            }

            if (fileNames.Count == 0)
            {
                if (ThrowExceptionWhenFileNotFound)
                {
                    var exception = new LocalFileReadException(caller, "local directory doesn't contain any matching files", Directory);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local directory doesn't contain any matching files: {0}",
                        Directory));

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
            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileRead, Directory, fileName.Replace(Directory, "", StringComparison.InvariantCultureIgnoreCase), null, null, null, null,
                "reading from local file {FileName}", fileName);

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

                var exception = new LocalFileReadException(caller, "error while opening local file", Directory, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening local file: {0}, message: {1}", fileName, ex.Message));
                throw exception;
            }
        }
    }
}