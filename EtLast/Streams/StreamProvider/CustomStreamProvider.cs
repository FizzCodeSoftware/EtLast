namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;

    public class CustomStreamProvider : IStreamProvider
    {
        public Func<Stream> StreamCreator { get; init; }
        public string StreamName { get; init; }

        public string Topic => StreamName;

        public IEnumerable<NamedStream> GetStreams(IProcess caller)
        {
            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.streamRead, StreamName, null, null, null, null,
                "reading from stream {StreamName}", StreamName);

            try
            {
                var stream = StreamCreator.Invoke();
                return new[]
                {
                    new NamedStream(StreamName, stream, iocUid, IoCommandKind.streamRead),
                };
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

                var exception = new EtlException(caller, "error while opening stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening stream: {0}, message: {1}", StreamName, ex.Message));
                exception.Data.Add("StreamName", StreamName);
                throw exception;
            }
        }
    }
}