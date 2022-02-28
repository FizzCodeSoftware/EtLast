namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

    public class MemorySinkProvider : ISinkProvider
    {
        public string Topic => null;

        public Func<MemoryStream> StreamCreator { get; init; }
        public string SinkName { get; init; }
        public string SinkLocation { get; init; }
        public string SinkPath { get; init; }

        /// <summary>
        /// Default value is false
        /// </summary>
        public bool AutomaticallyDispose { get; init; }

        public NamedSink GetSink(IProcess caller, string partitionKey)
        {
            var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.memoryWrite, SinkName, null, null, null, null,
                "writing to memory stream");

            try
            {
                var sinkUid = caller.Context.GetSinkUid(SinkLocation, SinkPath);

                var stream = StreamCreator.Invoke();
                return new NamedSink(SinkName, stream, iocUid, IoCommandKind.streamWrite, sinkUid);
            }
            catch (Exception ex)
            {
                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

                var exception = new EtlException(caller, "error while writing memory stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while writing memory stream: {0}, message: {1}", SinkName, ex.Message));
                exception.Data.Add("SinkName", SinkName);
                throw exception;
            }
        }
    }
}