namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

public class MemoryStreamProvider : IStreamProvider
{
    public Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _streamName = "MemoryStream";
    private readonly string _streamLocation = "memory";
    private readonly string _streamPath = "memory";

    public string GetTopic()
    {
        return _streamName;
    }

    public void Validate(IProcess caller)
    {
        if (StreamCreator == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(StreamCreator));

        if (_streamName == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamName));

        if (_streamLocation == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamLocation));

        if (_streamPath == null)
            throw new ProcessParameterNullException(caller, "StreamProvider." + nameof(_streamPath));
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.streamRead, _streamLocation, _streamPath, null, null, null, null,
            "reading from stream {StreamName}", _streamName);

        try
        {
            var stream = StreamCreator.Invoke();
            return new[]
            {
                new NamedStream(_streamName, stream, iocUid, IoCommandKind.streamRead),
            };
        }
        catch (Exception ex)
        {
            caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.fileRead, iocUid, null, ex);

            var exception = new EtlException(caller, "error while opening stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening stream: {0}, message: {1}",
                _streamName, ex.Message));

            exception.Data.Add("StreamName", _streamName);
            throw exception;
        }
    }
}
