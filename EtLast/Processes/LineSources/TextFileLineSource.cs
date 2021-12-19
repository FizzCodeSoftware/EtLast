namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class TextFileLineSource : ILineSource
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public string FileName { get; init; }

        /// <summary>
        /// Default value is false;
        /// </summary>
        public bool ThrowExceptionWhenFileNotFound { get; init; } = false;

        private int _iocUid;
        private int _resultCount;
        private Stream _stream;
        private StreamReader _reader;

        public int GetIoCommandUid()
        {
            return _iocUid;
        }

        public void Prepare(IProcess caller)
        {
            _iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.fileRead, FileName, null, null, null, null,
                "reading from {FileName}",
                PathHelpers.GetFriendlyPathName(FileName));

            _resultCount = 0;

            if (!File.Exists(FileName))
            {
                if (ThrowExceptionWhenFileNotFound)
                {
                    var exception = new FileReadException(caller, "input file doesn't exist", FileName);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exist: {0}",
                        FileName));

                    exception.Data.Add("FileName", FileName);
                    caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, _iocUid, 0, exception);
                    throw exception;
                }

                caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.fileRead, _iocUid, 0);
                return;
            }

            try
            {
                _stream = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                _reader = new StreamReader(_stream);
            }
            catch (Exception ex)
            {
                _stream = null;
                _reader = null;
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, _iocUid, null, ex);

                var exception = new EtlException(caller, "error while opening delimited file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening delimited file: {0}, message: {1}", FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }

        public string ReadLine(IProcess caller)
        {
            if (_reader == null)
                return null;

            try
            {
                var line = _reader.ReadLine();
                if (line == null)
                {
                    Release(caller);
                    return null;
                }

                _resultCount++;
                return line;
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, _iocUid, _resultCount, ex);
                var exception = new EtlException(caller, "error while reading delimited data from file", ex);
                exception.Data.Add("FileName", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading delimited data from file: {0}, message: {1}", FileName, ex.Message));
                throw exception;
            }
        }

        public void Release(IProcess caller)
        {
            if (_reader != null)
            {
                _reader.Dispose();
                _stream.Dispose();
                _reader = null;
                _stream = null;
                caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.fileRead, _iocUid, _resultCount);
            }
        }

        public string GetTopic()
        {
            if (FileName == null)
                return null;

            return Path.GetFileName(FileName);
        }
    }
}