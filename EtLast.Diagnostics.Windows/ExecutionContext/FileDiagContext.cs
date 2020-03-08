namespace FizzCode.EtLast.Diagnostics
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using FizzCode.EtLast.Diagnostics.Interface;

    [DebuggerDisplay("{Name}")]
    public class FileDiagContext : StagingDiagContext
    {
        public string DataFolder { get; }
        public int LastMainFileIndex { get; set; }
        public long LastMainFileSize { get; set; } = 0;
        public override bool FullyLoaded => EndedOn != null && _stagedEvents.Count == 0;

        private readonly List<AbstractEvent> _stagedEvents = new List<AbstractEvent>();
        private readonly Dictionary<string, FileStream> _openStoreWriterStreams = new Dictionary<string, FileStream>();

        public FileDiagContext(DiagSession session, string name, DateTime startedOn, string dataFolder, int fileCount)
            : base(session, name, startedOn)
        {
            DataFolder = dataFolder;
            LastMainFileIndex = fileCount;
        }

        private string GetMainFileName(int index)
        {
            return Path.Combine(DataFolder, "main-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
        }

        private string GetStoreFileName(int storeUid)
        {
            return Path.Combine(DataFolder, "store-" + storeUid.ToString("D", CultureInfo.InvariantCulture) + ".bin");
        }

        public override void StageEvents(MemoryStream input)
        {
            var length = input.Length;

            if (LastMainFileSize + length > 1024 * 1024 * 50)
            {
                LastMainFileIndex++;
                LastMainFileSize = 0;
            }

            var mainFileName = GetMainFileName(LastMainFileIndex);
            using (var fw = new FileStream(mainFileName, FileMode.Append, FileAccess.Write))
            {
                input.CopyTo(fw);
                LastMainFileSize += length;
            }

            input.Position = 0;

            var events = new List<AbstractEvent>();
            //var storeWriterStreams = new Dictionary<string, FileStream>();

            using (var reader = new ExtendedBinaryReader(input, Encoding.UTF8))
            {
                while (reader.BaseStream.Position < length)
                {
                    var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                    if (eventKind == DiagnosticsEventKind.TextDictionaryKeyAdded)
                    {
                        var key = reader.Read7BitEncodedInt();
                        var text = reader.ReadNullableString();
                        TextDictionary.Add(key, text == null ? null : string.Intern(text));
                        continue;
                    }

                    var position = input.Position;
                    var eventDataSize = reader.ReadInt32(); // eventDataSize
                    var timestamp = reader.ReadInt64();

                    if (eventKind == DiagnosticsEventKind.ContextEnded)
                    {
                        foreach (var storeStream in _openStoreWriterStreams)
                        {
                            storeStream.Value.Flush();
                            storeStream.Value.Dispose();
                        }

                        _openStoreWriterStreams.Clear();
                        EndedOn = new DateTime(timestamp);
                        continue;
                    }

                    var evt = eventKind switch
                    {
                        DiagnosticsEventKind.Log => ReadLogEvent(reader),
                        DiagnosticsEventKind.RowCreated => ReadRowCreatedEvent(reader),
                        DiagnosticsEventKind.RowOwnerChanged => ReadRowOwnerChangedEvent(reader),
                        DiagnosticsEventKind.RowValueChanged => ReadRowValueChangedEvent(reader),
                        DiagnosticsEventKind.RowStoreStarted => ReadRowStoreStartedEvent(reader),
                        DiagnosticsEventKind.RowStored => ReadRowStoredEvent(reader),
                        DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                        DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                        DiagnosticsEventKind.IoCommandStart => ReadIoCommandStartEvent(reader),
                        DiagnosticsEventKind.IoCommandEnd => ReadIoCommandEndEvent(reader),
                        _ => null,
                    };

                    evt.Timestamp = timestamp;
                    events.Add(evt);

                    if (evt is RowStoredEvent rse)
                    {
                        var eventBytes = input.ReadFrom(position, eventDataSize + 4);

                        var storeFileName = GetStoreFileName(rse.StoreUID);
                        if (!_openStoreWriterStreams.TryGetValue(storeFileName, out var storeWriterStream))
                        {
                            storeWriterStream = new FileStream(storeFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 512 * 1024);
                            _openStoreWriterStreams.Add(storeFileName, storeWriterStream);
                        }

                        lock (_openStoreWriterStreams)
                        {
                            storeWriterStream.Write(eventBytes, 0, eventBytes.Length);
                        }
                    }
                }
            }

            lock (_stagedEvents)
            {
                _stagedEvents.AddRange(events);
            }
        }

        public override void LoadStagedEvents()
        {
            List<AbstractEvent> newEvents;
            lock (_stagedEvents)
            {
                newEvents = new List<AbstractEvent>(_stagedEvents);
                _stagedEvents.Clear();
            }

            WholePlaybook.AddEvents(newEvents);
        }

        public void LoadFiles()
        {
            var fileNames = Enumerable
                .Range(0, LastMainFileIndex + 1)
                .Select(GetMainFileName);

            foreach (var fileName in fileNames)
            {
                using (var memoryCache = new MemoryStream(File.ReadAllBytes(fileName)))
                using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
                {
                    var events = new List<AbstractEvent>();

                    var length = memoryCache.Length;
                    while (memoryCache.Position < length)
                    {
                        var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                        if (eventKind == DiagnosticsEventKind.TextDictionaryKeyAdded)
                        {
                            var key = reader.Read7BitEncodedInt();
                            var text = reader.ReadNullableString();
                            TextDictionary.Add(key, text == null ? null : string.Intern(text));
                            continue;
                        }

                        var eventDataSize = reader.ReadInt32();
                        if (memoryCache.Position + eventDataSize > memoryCache.Length)
                            break;

                        var timestamp = reader.ReadInt64();

                        if (eventKind == DiagnosticsEventKind.ContextEnded)
                        {
                            EndedOn = new DateTime(timestamp);
                            continue;
                        }

                        var evt = eventKind switch
                        {
                            DiagnosticsEventKind.Log => ReadLogEvent(reader),
                            DiagnosticsEventKind.RowCreated => ReadRowCreatedEvent(reader),
                            DiagnosticsEventKind.RowOwnerChanged => ReadRowOwnerChangedEvent(reader),
                            DiagnosticsEventKind.RowValueChanged => ReadRowValueChangedEvent(reader),
                            DiagnosticsEventKind.RowStoreStarted => ReadRowStoreStartedEvent(reader),
                            DiagnosticsEventKind.RowStored => ReadRowStoredEvent(reader),
                            DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                            DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                            DiagnosticsEventKind.IoCommandStart => ReadIoCommandStartEvent(reader),
                            DiagnosticsEventKind.IoCommandEnd => ReadIoCommandEndEvent(reader),
                            _ => null,
                        };

                        evt.Timestamp = timestamp;
                        events.Add(evt);
                    }

                    WholePlaybook.AddEvents(events);
                }
            }
        }

        public override void EnumerateThroughStoredRows(int storeUid, Action<RowStoredEvent> callback)
        {
            var storeFileName = GetStoreFileName(storeUid);

            if (!File.Exists(storeFileName))
                return;

            if (_openStoreWriterStreams.TryGetValue(storeFileName, out var storeWriterStream))
            {
                lock (_openStoreWriterStreams)
                {
                    storeWriterStream.Flush();
                }
            }

            using (var fileStream = new FileStream(storeFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var memoryCache = new MemoryStream())
            {
                fileStream.CopyTo(memoryCache);
                memoryCache.Position = 0;
                using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
                {
                    var length = memoryCache.Length;
                    while (memoryCache.Position < length)
                    {
                        var eventDataSize = reader.ReadInt32();
                        if (memoryCache.Position + eventDataSize > memoryCache.Length)
                            break;

                        var timestamp = reader.ReadInt64();
                        var evt = ReadRowStoredEvent(reader);
                        evt.Timestamp = timestamp;
                        callback.Invoke(evt);
                    }
                }
            }
        }

        public override void EnumerateThroughEvents(Action<AbstractEvent> callback, params DiagnosticsEventKind[] eventKindFilter)
        {
            var fileNames = Enumerable
                .Range(0, LastMainFileIndex + 1)
                .Select(GetMainFileName);

            var eventKinds = eventKindFilter.ToHashSet();

            foreach (var fileName in fileNames)
            {
                using (var memoryCache = new MemoryStream(File.ReadAllBytes(fileName)))
                using (var reader = new ExtendedBinaryReader(memoryCache, Encoding.UTF8))
                {
                    var length = reader.BaseStream.Length;
                    while (reader.BaseStream.Position < length)
                    {
                        var eventKind = (DiagnosticsEventKind)reader.ReadByte();
                        if (eventKind == DiagnosticsEventKind.TextDictionaryKeyAdded)
                        {
                            var key = reader.Read7BitEncodedInt();
                            var text = reader.ReadNullableString();
                            continue;
                        }

                        var eventDataSize = reader.ReadInt32();

                        if (eventKind == DiagnosticsEventKind.ContextEnded)
                            continue;

                        if (eventKinds.Contains(eventKind))
                        {
                            var timestamp = reader.ReadInt64();
                            var evt = eventKind switch
                            {
                                DiagnosticsEventKind.Log => ReadLogEvent(reader),
                                DiagnosticsEventKind.RowCreated => ReadRowCreatedEvent(reader),
                                DiagnosticsEventKind.RowOwnerChanged => ReadRowOwnerChangedEvent(reader),
                                DiagnosticsEventKind.RowValueChanged => ReadRowValueChangedEvent(reader),
                                DiagnosticsEventKind.RowStoreStarted => ReadRowStoreStartedEvent(reader),
                                DiagnosticsEventKind.RowStored => ReadRowStoredEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                                DiagnosticsEventKind.IoCommandStart => ReadIoCommandStartEvent(reader),
                                DiagnosticsEventKind.IoCommandEnd => ReadIoCommandEndEvent(reader),
                                _ => null,
                            };

                            evt.Timestamp = timestamp;
                            callback.Invoke(evt);
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