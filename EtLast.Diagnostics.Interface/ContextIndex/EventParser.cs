#pragma warning disable CA1822 // Mark members as static
namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class EventParser
    {
        public Dictionary<int, string> TextDictionary { get; }

        public EventParser()
        {
            TextDictionary = new Dictionary<int, string>()
            {
                [0] = null,
            };
        }

        public ProcessInvocationStartEvent ReadProcessInvocationStartEvent(ExtendedBinaryReader reader)
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

        public ProcessInvocationEndEvent ReadProcessInvocationEndEvent(ExtendedBinaryReader reader)
        {
            return new ProcessInvocationEndEvent
            {
                InvocationUID = reader.Read7BitEncodedInt(),
                ElapsedMilliseconds = reader.ReadInt64(),
                NetTimeMilliseconds = reader.ReadNullableInt64(),
            };
        }

        public IoCommandStartEvent ReadIoCommandStartEvent(ExtendedBinaryReader reader)
        {
            var evt = new IoCommandStartEvent
            {
                Uid = reader.Read7BitEncodedInt(),
                ProcessInvocationUid = reader.Read7BitEncodedInt(),
                Kind = (IoCommandKind)reader.ReadByte(),
                Location = TextDictionary[reader.Read7BitEncodedInt()],
                Path = TextDictionary[reader.Read7BitEncodedInt()],
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

        public IoCommandEndEvent ReadIoCommandEndEvent(ExtendedBinaryReader reader)
        {
            var evt = new IoCommandEndEvent
            {
                Uid = reader.Read7BitEncodedInt(),
                AffectedDataCount = reader.ReadNullableInt32(),
                ErrorMessage = reader.ReadNullableString(),
            };

            return evt;
        }

        public RowCreatedEvent ReadRowCreatedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowCreatedEvent
            {
                ProcessInvocationUid = reader.Read7BitEncodedInt(),
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

        public RowOwnerChangedEvent ReadRowOwnerChangedEvent(ExtendedBinaryReader reader)
        {
            return new RowOwnerChangedEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                PreviousProcessInvocationUid = reader.Read7BitEncodedInt(),
                NewProcessInvocationUid = reader.ReadNullableInt32()
            };
        }

        public RowValueChangedEvent ReadRowValueChangedEvent(ExtendedBinaryReader reader)
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

        public RowStoreStartedEvent ReadRowStoreStartedEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowStoreStartedEvent
            {
                UID = reader.Read7BitEncodedInt(),
                Location = TextDictionary[reader.Read7BitEncodedInt()],
                Path = TextDictionary[reader.Read7BitEncodedInt()],
            };

            return evt;
        }

        public RowStoredEvent ReadRowStoredEvent(ExtendedBinaryReader reader)
        {
            var evt = new RowStoredEvent
            {
                RowUid = reader.Read7BitEncodedInt(),
                ProcessInvocationUID = reader.Read7BitEncodedInt(),
                StoreUid = reader.Read7BitEncodedInt()
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

        public LogEvent ReadLogEvent(ExtendedBinaryReader reader)
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
#pragma warning restore CA1822 // Mark members as static