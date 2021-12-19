namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class TextStreamLineSource : ILineSource
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public NamedStream Stream { get; init; }

        /// <summary>
        /// Default value is false;
        /// </summary>
        public bool ThrowExceptionWithoutStream { get; init; } = false;

        private int _iocUid;
        private int _resultCount;
        private StreamReader _reader;

        public int GetIoCommandUid()
        {
            return _iocUid;
        }

        public void Prepare(IProcess caller)
        {
            _iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.streamRead, Stream.Name, null, null, null, null,
                "reading from {StreamName}", Stream.Name);

            _resultCount = 0;

            if (Stream?.Stream == null)
            {
                if (ThrowExceptionWithoutStream)
                {
                    var exception = new StreamReadException(caller, "input stream is not set", Stream);
                    caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.streamRead, _iocUid, 0, exception);
                    throw exception;
                }

                caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.streamRead, _iocUid, 0);
                return;
            }

            _reader = new StreamReader(Stream.Stream);
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
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.streamRead, _iocUid, _resultCount, ex);
                var exception = new EtlException(caller, "error while reading delimited data from stream", ex);
                exception.Data.Add("StreamName", Stream.Name);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading delimited data from stream: {0}, message: {1}", Stream.Name, ex.Message));
                throw exception;
            }
        }

        public void Release(IProcess caller)
        {
            if (_reader != null)
            {
                _reader.Dispose();
                Stream.Dispose();
                _reader = null;
                caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.streamRead, _iocUid, _resultCount);
            }
        }

        public string GetTopic()
        {
            return Stream?.Name;
        }
    }
}