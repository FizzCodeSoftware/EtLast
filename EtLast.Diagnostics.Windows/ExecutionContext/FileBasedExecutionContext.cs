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

    public delegate void OnExecutionContextStartedOnSetDelegate(FileBasedExecutionContext executionContext);

    [DebuggerDisplay("{Name}")]
    public class FileBasedExecutionContext : AbstractExecutionContext
    {
        public string DataFolder { get; }
        public int LastMainFileIndex { get; set; }
        public long LastMainFileSize { get; set; } = 0;

        private readonly List<AbstractEvent> _stagedEvents = new List<AbstractEvent>();
        private readonly Dictionary<string, FileStream> _storeWriterStreams = new Dictionary<string, FileStream>();

        public FileBasedExecutionContext(Session session, string name, DateTime startedOn, string dataFolder, int fileCount)
            : base(session, name, startedOn)
        {
            DataFolder = dataFolder;
            LastMainFileIndex = fileCount;
        }

        public string GetMainFileName(int index)
        {
            return Path.Combine(DataFolder, "main-" + index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
        }

        public string GetStoreFileName(int storeUID)
        {
            return Path.Combine(DataFolder, "store-" + storeUID.ToString("D", CultureInfo.InvariantCulture) + ".bin");
        }

        public void StageEvents(MemoryStream input)
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
                        foreach (var storeStream in _storeWriterStreams)
                        {
                            storeStream.Value.Flush();
                            storeStream.Value.Dispose();
                        }

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
                        DiagnosticsEventKind.ContextCountersUpdated => ReadContextCountersUpdatedEvent(reader),
                        DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                        DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                        DiagnosticsEventKind.DataStoreCommand => ReadDataStoreCommandEvent(reader),
                        _ => null,
                    };

                    evt.Timestamp = timestamp;
                    events.Add(evt);

                    if (evt is RowStoredEvent rse)
                    {
                        var eventBytes = input.ReadFrom(position, eventDataSize + 4);

                        var storeFileName = GetStoreFileName(rse.StoreUID);
                        if (!_storeWriterStreams.TryGetValue(storeFileName, out var storeStream))
                        {
                            storeStream = new FileStream(storeFileName, FileMode.Append, FileAccess.Write);
                            _storeWriterStreams.Add(storeFileName, storeStream);
                        }

                        storeStream.Write(eventBytes, 0, eventBytes.Length);
                    }
                }
            }

            lock (_stagedEvents)
            {
                _stagedEvents.AddRange(events);
            }
        }

        public void LoadStagedEvents()
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
                        if (memoryCache.Position + eventDataSize >= memoryCache.Length)
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
                            DiagnosticsEventKind.ContextCountersUpdated => ReadContextCountersUpdatedEvent(reader),
                            DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                            DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                            DiagnosticsEventKind.DataStoreCommand => ReadDataStoreCommandEvent(reader),
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
            var fileName = GetStoreFileName(storeUid);

            using (var memoryCache = new MemoryStream(File.ReadAllBytes(fileName)))
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
                                DiagnosticsEventKind.ContextCountersUpdated => ReadContextCountersUpdatedEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationStart => ReadProcessInvocationStartEvent(reader),
                                DiagnosticsEventKind.ProcessInvocationEnd => ReadProcessInvocationEndEvent(reader),
                                DiagnosticsEventKind.DataStoreCommand => ReadDataStoreCommandEvent(reader),
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

        protected virtual AbstractEvent ReadProcessInvocationStartEvent(ExtendedBinaryReader reader)
        {
            return new ProcessInvocationStartEvent
            {
                InvocationUID = reader.Read7BitEncodedInt(),
                InstanceUID = reader.Read7BitEncodedInt(),
                InvocationCounter = reader.Read7BitEncodedInt(),
                Type = reader.ReadString(),
                Kind = (ProcessKind)reader.ReadByte(),
                Name = reader.ReadString(),
                Topic = reader.ReadNullableString(),
                CallerInvocationUID = reader.ReadNullableInt32()
            };
        }

        protected virtual AbstractEvent ReadProcessInvocationEndEvent(ExtendedBinaryReader reader)
        {
            return new ProcessInvocationEndEvent
            {
                InvocationUID = reader.Read7BitEncodedInt(),
                ElapsedMilliseconds = reader.ReadInt64()
            };
        }

        protected virtual AbstractEvent ReadDataStoreCommandEvent(ExtendedBinaryReader reader)
        {
            var evt = new DataStoreCommandEvent
            {
                ProcessInvocationUID = reader.Read7BitEncodedInt(),
                Kind = (DataStoreCommandKind)reader.ReadByte(),
                Location = TextDictionary[reader.Read7BitEncodedInt()],
                Command = reader.ReadString(),
                TransactionId = TextDictionary[reader.Read7BitEncodedInt()],
            };

            var argCount = reader.Read7BitEncodedInt();
            if (argCount > 0)
            {
                evt.Arguments = new KeyValuePair<string, object>[argCount];
                for (var i = 0; i < argCount; i++)
                {
                    var name = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Arguments[i] = new KeyValuePair<string, object>(name, value);
                }
            }

            return evt;
        }

        protected virtual AbstractEvent ReadRowCreatedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowCreatedEvent
            {
                ProcessInvocationUID = reader.Read7BitEncodedInt(),
                RowUid = reader.Read7BitEncodedInt()
            };

            var columnCount = reader.Read7BitEncodedInt();
            if (columnCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[columnCount];
                for (var i = 0; i < columnCount; i++)
                {
                    var column = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        protected virtual AbstractEvent ReadRowOwnerChangedEvent(ExtendedBinaryReader reader)
        {
            return new RowOwnerChangedEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                PreviousProcessInvocationUID = reader.Read7BitEncodedInt(),
                NewProcessInvocationUID = reader.ReadNullableInt32()
            };
        }

        protected virtual AbstractEvent ReadRowValueChangedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowValueChangedEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                ProcessInvocationUID = reader.ReadNullableInt32()
            };

            var columnCount = reader.Read7BitEncodedInt();
            if (columnCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[columnCount];
                for (var i = 0; i < columnCount; i++)
                {
                    var column = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        protected virtual AbstractEvent ReadRowStoreStartedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowStoreStartedEvent()
            {
                UID = reader.Read7BitEncodedInt(),
            };

            var descriptorCount = reader.Read7BitEncodedInt();

            evt.Descriptor = new KeyValuePair<string, string>[descriptorCount];
            for (var i = 0; i < descriptorCount; i++)
            {
                var key = TextDictionary[reader.Read7BitEncodedInt()];
                var value = TextDictionary[reader.Read7BitEncodedInt()];
                evt.Descriptor[i] = new KeyValuePair<string, string>(key, value);
            }

            return evt;
        }

        protected virtual RowStoredEvent ReadRowStoredEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowStoredEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                ProcessInvocationUID = reader.Read7BitEncodedInt(),
                StoreUID = reader.Read7BitEncodedInt()
            };

            var columnCount = reader.Read7BitEncodedInt();
            if (columnCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[columnCount];
                for (var i = 0; i < columnCount; i++)
                {
                    var column = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        protected virtual AbstractEvent ReadContextCountersUpdatedEvent(ExtendedBinaryReader reader)
        {
            var evt = new ContextCountersUpdatedEvent();
            var counterCount = reader.Read7BitEncodedInt();
            evt.Counters = new Counter[counterCount];
            for (var i = 0; i < counterCount; i++)
            {
                evt.Counters[i] = new Counter()
                {
                    Name = TextDictionary[reader.Read7BitEncodedInt()],
                    Value = reader.ReadInt64(),
                    ValueType = (StatCounterValueType)reader.ReadByte(),
                };
            }

            return evt;
        }

        protected virtual AbstractEvent ReadLogEvent(ExtendedBinaryReader reader)
        {
            var evt = new LogEvent
            {
                Text = reader.ReadString(),
                Severity = (LogSeverity)reader.ReadByte(),
                ProcessInvocationUID = reader.ReadNullableInt32()
            };

            var argCount = reader.Read7BitEncodedInt();
            if (argCount > 0)
            {
                evt.Arguments = new KeyValuePair<string, object>[argCount];
                for (var i = 0; i < argCount; i++)
                {
                    var key = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Arguments[i] = new KeyValuePair<string, object>(key, value);
                }
            }

            return evt;
        }
    }
}