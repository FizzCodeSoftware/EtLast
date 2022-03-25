#pragma warning disable CA1001 // Types that own disposable fields should be disposable
namespace FizzCode.EtLast.Diagnostics.Interface;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

public class ContextIndex
{
    public string DataFolder { get; }
    private int _lastMainFileIndex;
    private long _lastMainFileSize;

    private readonly Dictionary<int, FileStream> _openSinkWriterStreams = new();
    private readonly object _openSinkWriterStreamsLock = new();

    private FileStream _rowEventStream;
    private int _lastRowEventFileIndex;
    private int _lastRowEventFileSize;
    private readonly object _rowEventStreamLock = new();

    private readonly Dictionary<int, ExtendedBinaryWriter> _processRowMapWriters = new();
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
        return Path.Combine(DataFolder, "main-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
    }

    private string GetRowEventFileName(int index)
    {
        return Path.Combine(DataFolder, "row-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
    }

    private string GetSinkFileName(int sinkUid)
    {
        return Path.Combine(DataFolder, "sink-" + sinkUid.ToString("D", CultureInfo.InvariantCulture) + ".bin");
    }

    private string GetProcessRowMapFileName(int processInvocationUid)
    {
        return Path.Combine(DataFolder, "process-row-map-" + processInvocationUid.ToString("D", CultureInfo.InvariantCulture) + ".bin");
    }

    public List<AbstractEvent> Append(MemoryStream input)
    {
        var length = input.Length;

        if (_lastMainFileSize + length > 1024 * 1024 * 25)
        {
            _lastMainFileIndex++;
            _lastMainFileSize = 0;
        }

        var mainFileName = GetMainFileName(_lastMainFileIndex);
        using (var fw = new FileStream(mainFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            input.CopyTo(fw);
            _lastMainFileSize += length;
        }

        input.Position = 0;

        var events = new List<AbstractEvent>();

        using (var reader = new ExtendedBinaryReader(input, Encoding.UTF8))
        {
            while (reader.BaseStream.Position < length)
            {
                var startPosition = input.Position;
                var eventKind = (DiagnosticsEventKind)reader.ReadByte();

                var eventDataPosition = input.Position;
                var eventDataSize = reader.ReadInt32(); // eventDataSize

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
                    DiagnosticsEventKind.RowOwnerChanged => _eventParser.ReadRowOwnerChangedEvent(reader),
                    DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                    DiagnosticsEventKind.SinkStarted => _eventParser.ReadSinkStartedEvent(reader),
                    DiagnosticsEventKind.WriteToSink => _eventParser.ReadWriteToSinkEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationStart => _eventParser.ReadProcessInvocationStartEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationEnd => _eventParser.ReadProcessInvocationEndEvent(reader),
                    DiagnosticsEventKind.IoCommandStart => _eventParser.ReadIoCommandStartEvent(reader),
                    DiagnosticsEventKind.IoCommandEnd => _eventParser.ReadIoCommandEndEvent(reader),
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
                else if (evt is RowCreatedEvent || evt is RowValueChangedEvent || evt is RowOwnerChangedEvent)
                {
                    var eventBytes = input.ReadFrom(startPosition, (int)(input.Position - startPosition));

                    lock (_rowEventStreamLock)
                    {
                        if (_lastRowEventFileSize + eventBytes.Length > 1024 * 1024 * 25)
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

                            writer.Write7BitEncodedInt((evt as AbstractRowEvent).RowUid);
                        }
                    }
                }
            }
        }

        return events;
    }

    public void EnumerateThroughSink(int sinkUid, Action<WriteToSinkEvent> callback)
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
                    var eventDataSize = reader.ReadInt32();
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
            .Range(0, _lastMainFileIndex + 1)
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
                        var eventDataSize = reader.ReadInt32();
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
                                DiagnosticsEventKind.RowOwnerChanged => _eventParser.ReadRowOwnerChangedEvent(reader),
                                DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                                DiagnosticsEventKind.SinkStarted => _eventParser.ReadSinkStartedEvent(reader),
                                DiagnosticsEventKind.WriteToSink => _eventParser.ReadWriteToSinkEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationStart => _eventParser.ReadProcessInvocationStartEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationEnd => _eventParser.ReadProcessInvocationEndEvent(reader),
                                DiagnosticsEventKind.IoCommandStart => _eventParser.ReadIoCommandStartEvent(reader),
                                DiagnosticsEventKind.IoCommandEnd => _eventParser.ReadIoCommandEndEvent(reader),
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

    public HashSet<int> GetProcessRowMap(int processInvocationUid)
    {
        var result = new HashSet<int>();

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
                        var rowUid = reader.Read7BitEncodedInt();
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
                    var length = memoryCache.Length;
                    while (memoryCache.Position + 5 < length)
                    {
                        var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                        var eventDataSize = reader.ReadInt32();
                        if (memoryCache.Position + eventDataSize > length)
                            break;

                        eventsRead++;

                        if (eventKinds.Contains(eventKind))
                        {
                            var timestamp = reader.ReadInt64();
                            var evt = eventKind switch
                            {
                                DiagnosticsEventKind.RowCreated => (AbstractRowEvent)_eventParser.ReadRowCreatedEvent(reader),
                                DiagnosticsEventKind.RowValueChanged => _eventParser.ReadRowValueChangedEvent(reader),
                                DiagnosticsEventKind.RowOwnerChanged => _eventParser.ReadRowOwnerChangedEvent(reader),
                                _ => null,
                            };

                            evt.Timestamp = timestamp;
                            var canContinue = callback.Invoke(evt);
                            if (!canContinue)
                            {
                                Debug.WriteLine("row events read: " + eventsRead.ToString("D", CultureInfo.InvariantCulture));
                                return;
                            }
                        }
                        else
                        {
                            reader.BaseStream.Seek(eventDataSize, SeekOrigin.Current);
                        }
                    }
                }
            }
        }

        Debug.WriteLine("row events read: " + eventsRead.ToString("D", CultureInfo.InvariantCulture));
    }
}
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
