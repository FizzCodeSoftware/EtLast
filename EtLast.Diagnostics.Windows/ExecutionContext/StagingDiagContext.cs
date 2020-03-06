namespace FizzCode.EtLast.Diagnostics
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using FizzCode.EtLast.Diagnostics.Interface;

    [DebuggerDisplay("{Name}")]
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public abstract class StagingDiagContext : AbstractDiagContext
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        protected StagingDiagContext(DiagSession session, string name, System.DateTime startedOn)
            : base(session, name, startedOn)
        {
        }

        public abstract void StageEvents(MemoryStream input);

        public abstract void LoadStagedEvents();

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
                ElapsedMilliseconds = reader.ReadInt64(),
                NetTimeMilliseconds = reader.ReadNullableInt64(),
            };
        }

        protected virtual AbstractEvent ReadIoCommandStartEvent(ExtendedBinaryReader reader)
        {
            var evt = new IoCommandStartEvent
            {
                Uid = reader.Read7BitEncodedInt(),
                ProcessInvocationUid = reader.Read7BitEncodedInt(),
                Kind = (IoCommandKind)reader.ReadByte(),
                Location = TextDictionary[reader.Read7BitEncodedInt()],
                TimeoutSeconds = reader.ReadNullableInt32(),
                Command = reader.ReadNullableString(),
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

        protected virtual AbstractEvent ReadIoCommandEndEvent(ExtendedBinaryReader reader)
        {
            var evt = new IoCommandEndEvent
            {
                Uid = reader.Read7BitEncodedInt(),
                AffectedDataCount = reader.ReadNullableInt32(),
                ErrorMessage = reader.ReadNullableString(),
            };

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
                TransactionId = TextDictionary[reader.Read7BitEncodedInt()],
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