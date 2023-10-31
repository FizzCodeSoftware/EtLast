namespace FizzCode.EtLast.Diagnostics.Interface;

public class ContextIndex
{
    public string DataFolder { get; }
    private int _lastStreamIndex;
    private long _lastStreamSize;

    private readonly Dictionary<long, FileStream> _openSinkWriterStreams = new();
    private readonly object _openSinkWriterStreamsLock = new();

    private FileStream _rowEventStream;
    private int _lastRowEventFileIndex;
    private int _lastRowEventFileSize;
    private readonly object _rowEventStreamLock = new();

    private readonly Dictionary<long, ExtendedBinaryWriter> _processRowMapWriters = new();
    private readonly object _processRowMapWritersLock = new();

    private readonly EventParser _eventParser = new();

    public DateTime StartedOn { get; }
    public DateTime? EndedOn { get; protected set; }

    public ContextIndex(string dataFolder)
    {
        DataFolder = dataFolder;
    }

    private string GetMainFileName(int index)
    {
        return Path.Combine(DataFolder, "stream-part-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
    }

    private string GetRowEventFileName(int index)
    {
        return Path.Combine(DataFolder, "row-part-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
    }

    private string GetSinkFileName(long sinkUid)
    {
        return Path.Combine(DataFolder, "sink-uid-" + sinkUid.ToString("D", CultureInfo.InvariantCulture) + ".bin");
    }

    private string GetProcessRowMapFileName(long processInvocationUid)
    {
        return Path.Combine(DataFolder, "process-rows-uid-" + processInvocationUid.ToString("D", CultureInfo.InvariantCulture) + ".bin");
    }

    public List<AbstractEvent> Append(MemoryStream input)
    {
        var length = input.Length;

        if (_lastStreamSize + length > 1024 * 1024 * 100)
        {
            _lastStreamIndex++;
            _lastStreamSize = 0;
        }

        var mainFileName = GetMainFileName(_lastStreamIndex);
        using (var fw = new FileStream(mainFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            input.CopyTo(fw);
            _lastStreamSize += length;
        }

        input.Position = 0;

        var events = new List<AbstractEvent>();

        using (var reader = new ExtendedBinaryReader(input, Encoding.UTF8))
        {
            while (input.Position < length)
            {
                var startPosition = input.Position;
                var eventKind = (DiagnosticsEventKind)reader.ReadByte();

                var eventDataPosition = input.Position;
                var eventDataSize = reader.Read7BitEncodedInt();

                //Debug.WriteLine(startPosition + "\t" + eventKind + "\t" + eventDataSize);

                if (eventKind == DiagnosticsEventKind.TextDictionaryKeyAdded)
                {
                    var key = reader.Read7BitEncodedInt();
                    var text = reader.ReadNullableString();
                    _eventParser.AddText(key, text);
                    continue;
                }

                var timestamp = reader.ReadInt64();

                if (eventKind == DiagnosticsEventKind.ContextEnded)
                {
                    foreach (var stream in _openSinkWriterStreams.Values)
                    {
                        stream.Flush();
                        stream.Dispose();
                    }

                    _openSinkWriterStreams.Clear();

                    if (_rowEventStream != null)
                    {
                        _rowEventStream.Flush();
                        _rowEventStream.Dispose();
                        _rowEventStream = null;
                    }

                    var writers = _processRowMapWriters.Values.ToList();
                    _processRowMapWriters.Clear();

                    foreach (var writer in writers)
                    {
                        writer.Flush();
                        writer.Dispose();
                    }

                    EndedOn = new DateTime(timestamp);
                    continue;
                }

                var evt = eventKind switch
                {
                    DiagnosticsEventKind.Log => (AbstractEvent)_eventParser.ReadLogEvent(reader),
                    DiagnosticsEventKind.RowCreated => _eventParser.ReadRowCreatedEvent(reader),
                    DiagnosticsEventKind.RowOwnerChanged => EventParser.ReadRowOwnerChangedEvent(reader),
                    DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                    DiagnosticsEventKind.SinkStarted => _eventParser.ReadSinkStartedEvent(reader),
                    DiagnosticsEventKind.WriteToSink => _eventParser.ReadWriteToSinkEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationStart => EventParser.ReadProcessInvocationStartEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationEnd => EventParser.ReadProcessInvocationEndEvent(reader),
                    DiagnosticsEventKind.IoCommandStart => _eventParser.ReadIoCommandStartEvent(reader),
                    DiagnosticsEventKind.IoCommandEnd => EventParser.ReadIoCommandEndEvent(reader),
                    _ => null,
                };

                evt.Timestamp = timestamp;
                events.Add(evt);

                if (evt is WriteToSinkEvent rse)
                {
                    var eventBytes = input.ReadFrom(eventDataPosition, (int)(input.Position - eventDataPosition));

                    lock (_openSinkWriterStreamsLock)
                    {
                        if (!_openSinkWriterStreams.TryGetValue(rse.SinkUID, out var sinkWriterStream))
                        {
                            var sinkFileName = GetSinkFileName(rse.SinkUID);
                            sinkWriterStream = new FileStream(sinkFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 512 * 1024);
                            _openSinkWriterStreams.Add(rse.SinkUID, sinkWriterStream);
                        }

                        sinkWriterStream.Write(eventBytes, 0, eventBytes.Length);
                    }
                }
                else if (evt is RowCreatedEvent or RowValueChangedEvent or RowOwnerChangedEvent)
                {
                    var eventBytes = input.ReadFrom(startPosition, (int)(input.Position - startPosition));

                    lock (_rowEventStreamLock)
                    {
                        if (_lastRowEventFileSize + eventBytes.Length > 1024 * 1024 * 100)
                        {
                            _lastRowEventFileIndex++;
                            _lastRowEventFileSize = 0;
                            if (_rowEventStream != null)
                            {
                                _rowEventStream.Flush();
                                _rowEventStream.Dispose();
                                _rowEventStream = null;
                            }
                        }

                        if (_rowEventStream == null)
                        {
                            var rowEventfileName = GetRowEventFileName(_lastRowEventFileIndex);
                            _rowEventStream = new FileStream(rowEventfileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 512 * 1024);
                        }
                    }

                    _rowEventStream.Write(eventBytes, 0, eventBytes.Length);
                    _lastRowEventFileSize += eventBytes.Length;

                    var involvedProcessUid = evt is RowCreatedEvent rce
                        ? rce.ProcessInvocationUid
                        : (evt is RowOwnerChangedEvent roce)
                            ? roce.NewProcessInvocationUid
                            : null;

                    if (involvedProcessUid != null)
                    {
                        lock (_processRowMapWritersLock)
                        {
                            if (!_processRowMapWriters.TryGetValue(involvedProcessUid.Value, out var writer))
                            {
                                var processRowMapFileName = GetProcessRowMapFileName(involvedProcessUid.Value);
                                var stream = new FileStream(processRowMapFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 128 * 1024);
                                writer = new ExtendedBinaryWriter(stream, Encoding.UTF8);
                                _processRowMapWriters.Add(involvedProcessUid.Value, writer);
                            }

                            writer.Write7BitEncodedInt64((evt as AbstractRowEvent).RowUid);
                        }
                    }
                }
            }
        }

        return events;
    }

    public void EnumerateThroughSink(long sinkUid, Action<WriteToSinkEvent> callback)
    {
        var fileName = GetSinkFileName(sinkUid);
        if (!File.Exists(fileName))
            return;

        lock (_openSinkWriterStreamsLock)
        {
            if (_openSinkWriterStreams.TryGetValue(sinkUid, out var sinkWriterStream))
            {
                sinkWriterStream.Flush();
            }
        }

        using (var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (var memoryCache = new MemoryStream())
        {
            fileStream.CopyTo(memoryCache);
            memoryCache.Position = 0;
            using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
            {
                var length = memoryCache.Length;
                while (memoryCache.Position + 5 < length)
                {
                    var eventDataSize = reader.Read7BitEncodedInt();
                    if (memoryCache.Position + eventDataSize > length)
                        break;

                    var timestamp = reader.ReadInt64();
                    var evt = _eventParser.ReadWriteToSinkEvent(reader);
                    evt.Timestamp = timestamp;
                    callback.Invoke(evt);
                }
            }
        }
    }

    public void EnumerateThroughEvents(Func<AbstractEvent, bool> callback, params DiagnosticsEventKind[] eventKindFilter)
    {
        var fileNames = Enumerable
            .Range(0, _lastStreamIndex + 1)
            .Select(GetMainFileName);

        var eventKinds = eventKindFilter.ToHashSet();

        var eventsRead = 0;
        foreach (var fileName in fileNames)
        {
            using (var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var memoryCache = new MemoryStream())
            {
                fileStream.CopyTo(memoryCache);
                memoryCache.Position = 0;

                using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
                {
                    var length = reader.BaseStream.Length;
                    while (memoryCache.Position + 5 < length)
                    {
                        var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                        var eventDataSize = reader.Read7BitEncodedInt();
                        if (memoryCache.Position + eventDataSize > length)
                            break;

                        eventsRead++;

                        if (eventKinds.Contains(eventKind))
                        {
                            var timestamp = reader.ReadInt64();
                            var evt = eventKind switch
                            {
                                DiagnosticsEventKind.Log => (AbstractEvent)_eventParser.ReadLogEvent(reader),
                                DiagnosticsEventKind.RowCreated => _eventParser.ReadRowCreatedEvent(reader),
                                DiagnosticsEventKind.RowOwnerChanged => EventParser.ReadRowOwnerChangedEvent(reader),
                                DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                                DiagnosticsEventKind.SinkStarted => _eventParser.ReadSinkStartedEvent(reader),
                                DiagnosticsEventKind.WriteToSink => _eventParser.ReadWriteToSinkEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationStart => EventParser.ReadProcessInvocationStartEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationEnd => EventParser.ReadProcessInvocationEndEvent(reader),
                                DiagnosticsEventKind.IoCommandStart => _eventParser.ReadIoCommandStartEvent(reader),
                                DiagnosticsEventKind.IoCommandEnd => EventParser.ReadIoCommandEndEvent(reader),
                                _ => null,
                            };

                            evt.Timestamp = timestamp;
                            var canContinue = callback.Invoke(evt);
                            if (!canContinue)
                                break;
                        }
                        else
                        {
                            reader.BaseStream.Seek(eventDataSize, SeekOrigin.Current);
                        }
                    }
                }
            }
        }

        Debug.WriteLine("events read: " + eventsRead.ToString("D", CultureInfo.InvariantCulture));
    }

    public HashSet<long> GetProcessRowMap(long processInvocationUid)
    {
        var result = new HashSet<long>();

        var fileName = GetProcessRowMapFileName(processInvocationUid);
        if (!File.Exists(fileName))
            return result;

        lock (_processRowMapWritersLock)
        {
            if (_processRowMapWriters.TryGetValue(processInvocationUid, out var writer))
            {
                writer.Flush();
            }
        }

        using (var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (var memoryCache = new MemoryStream())
        {
            fileStream.CopyTo(memoryCache);
            memoryCache.Position = 0;

            using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
            {
                var length = memoryCache.Length;
                while (memoryCache.Position < length)
                {
                    try
                    {
                        var rowUid = reader.Read7BitEncodedInt64();
                        result.Add(rowUid);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

        return result;
    }

    public void EnumerateThroughRowEvents(Func<AbstractRowEvent, bool> callback, params DiagnosticsEventKind[] eventKindFilter)
    {
        var fileNames = Enumerable
            .Range(0, _lastRowEventFileIndex + 1)
            .Select(GetRowEventFileName);

        var eventKinds = eventKindFilter.ToHashSet();

        lock (_rowEventStreamLock)
        {
            _rowEventStream?.Flush();
        }

        foreach (var fileName in fileNames)
        {
            using (var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var memoryCache = new MemoryStream())
            {
                fileStream.CopyTo(memoryCache);
                memoryCache.Position = 0;

                using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
                {
                    var length = memoryCache.Length;
                    while (memoryCache.Position + 5 < length)
                    {
                        var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                        var eventDataSize = reader.Read7BitEncodedInt();
                        if (memoryCache.Position + eventDataSize > length)
                            break;

                        if (eventKinds.Contains(eventKind))
                        {
                            var timestamp = reader.ReadInt64();
                            var evt = eventKind switch
                            {
                                DiagnosticsEventKind.RowCreated => (AbstractRowEvent)_eventParser.ReadRowCreatedEvent(reader),
                                DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                                DiagnosticsEventKind.RowOwnerChanged => EventParser.ReadRowOwnerChangedEvent(reader),
                                _ => null,
                            };

                            evt.Timestamp = timestamp;
                            var canContinue = callback.Invoke(evt);
                            if (!canContinue)
                                return;
                        }
                        else
                        {
                            reader.BaseStream.Seek(eventDataSize, SeekOrigin.Current);
                        }
                    }
                }
            }
        }
    }
}