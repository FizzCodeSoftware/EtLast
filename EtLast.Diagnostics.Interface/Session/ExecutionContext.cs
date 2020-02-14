namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;

    public delegate void OnExecutionContextStartedOnSetDelegate(ExecutionContext executionContext);

    [DebuggerDisplay("{Name}")]
    public class ExecutionContext
    {
        public string Name { get; }
        public Session Session { get; }
        public Playbook WholePlaybook { get; }
        public DateTime StartedOn { get; }
        public Dictionary<int, string> TextDictionary { get; }
        public string DataFolder { get; }
        public int FileCount { get; set; }
        public long LastWrittenFileSize { get; set; } = 0;

        private readonly List<AbstractEvent> _allEvents = new List<AbstractEvent>();
        private readonly List<AbstractEvent> _unprocessedEvents = new List<AbstractEvent>();

        public ExecutionContext(Session session, string name, string dataFolder, DateTime startedOn, int fileCount)
        {
            Session = session;
            Name = name;
            WholePlaybook = new Playbook(this);
            StartedOn = startedOn;
            TextDictionary = new Dictionary<int, string>()
            {
                [0] = null,
            };
            DataFolder = dataFolder;
        }

        public string GetBinaryFileName(int index)
        {
            return Path.Combine(DataFolder, index.ToString("D", CultureInfo.InvariantCulture)) + ".bin";
        }

        public void ProcessBinary(ExtendedBinaryReader reader)
        {
            var events = new List<AbstractEvent>();

            var length = reader.BaseStream.Length;
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

                var timestamp = reader.ReadInt64();

                var abstractEvent = eventKind switch
                {
                    DiagnosticsEventKind.Log => ProcessLogEvent(reader),
                    DiagnosticsEventKind.RowCreated => ProcessRowCreatedEvent(reader),
                    DiagnosticsEventKind.RowOwnerChanged => ProcessRowOwnerChangedEvent(reader),
                    DiagnosticsEventKind.RowValueChanged => ProcessRowValueChangedEvent(reader),
                    DiagnosticsEventKind.RowStored => ProcessRowStoredEvent(reader),
                    DiagnosticsEventKind.ContextCountersUpdated => ProcessContextCountersUpdatedEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationStart => ProcessProcessInvocationStartEvent(reader),
                    DiagnosticsEventKind.ProcessInvocationEnd => ProcessProcessInvocationEndEvent(reader),
                    DiagnosticsEventKind.DataStoreCommand => ProcessDataStoreCommandEvent(reader),
                    _ => null,
                };

                if (abstractEvent != null)
                {
                    abstractEvent.Timestamp = timestamp;
                    events.Add(abstractEvent);
                }
            }

            lock (_unprocessedEvents)
            {
                _unprocessedEvents.AddRange(events);
            }
        }

        public void ProcessEvents()
        {
            List<AbstractEvent> newEvents;
            lock (_unprocessedEvents)
            {
                newEvents = new List<AbstractEvent>(_unprocessedEvents);
                _unprocessedEvents.Clear();
            }

            WholePlaybook.AddEvents(newEvents);
            _allEvents.AddRange(newEvents);
        }

        public IEnumerable<AbstractEvent> GetEventsUntil(DateTime? latest)
        {
            if (latest == null)
            {
                foreach (var evt in _allEvents)
                    yield return evt;
            }
            else
            {
                var ticks = latest.Value.Ticks;
                foreach (var evt in _allEvents)
                {
                    if (evt.Timestamp > ticks)
                        yield break;

                    yield return evt;
                }
            }
        }

        private AbstractEvent ProcessProcessInvocationStartEvent(ExtendedBinaryReader reader)
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

        private AbstractEvent ProcessProcessInvocationEndEvent(ExtendedBinaryReader reader)
        {
            return new ProcessInvocationEndEvent
            {
                InvocationUID = reader.Read7BitEncodedInt(),
                ElapsedMilliseconds = reader.ReadInt64()
            };
        }

        private AbstractEvent ProcessDataStoreCommandEvent(ExtendedBinaryReader reader)
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

        private AbstractEvent ProcessRowCreatedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowCreatedEvent
            {
                ProcessInvocationUID = reader.Read7BitEncodedInt(),
                RowUid = reader.Read7BitEncodedInt()
            };

            var valueCount = reader.Read7BitEncodedInt();
            if (valueCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[valueCount];
                for (var i = 0; i < valueCount; i++)
                {
                    var column = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        private AbstractEvent ProcessRowOwnerChangedEvent(ExtendedBinaryReader reader)
        {
            return new RowOwnerChangedEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                PreviousProcessInvocationUID = reader.Read7BitEncodedInt(),
                NewProcessInvocationUID = reader.ReadNullableInt32()
            };
        }

        private AbstractEvent ProcessRowValueChangedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowValueChangedEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                ProcessInvocationUID = reader.ReadNullableInt32()
            };

            var valueCount = reader.Read7BitEncodedInt();
            if (valueCount > 0)
            {
                evt.Values = new KeyValuePair<string, object>[valueCount];
                for (var i = 0; i < valueCount; i++)
                {
                    var column = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = reader.ReadObject();
                    evt.Values[i] = new KeyValuePair<string, object>(column, value);
                }
            }

            return evt;
        }

        private AbstractEvent ProcessRowStoredEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowStoredEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                ProcessInvocationUID = reader.Read7BitEncodedInt()
            };

            var locationCount = reader.Read7BitEncodedInt();
            if (locationCount > 0)
            {
                evt.Locations = new KeyValuePair<string, string>[locationCount];
                for (var i = 0; i < locationCount; i++)
                {
                    var key = TextDictionary[reader.Read7BitEncodedInt()];
                    var value = TextDictionary[reader.Read7BitEncodedInt()];
                    evt.Locations[i] = new KeyValuePair<string, string>(key, value);
                }
            }

            return evt;
        }

        private AbstractEvent ProcessContextCountersUpdatedEvent(ExtendedBinaryReader reader)
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

        private AbstractEvent ProcessLogEvent(ExtendedBinaryReader reader)
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