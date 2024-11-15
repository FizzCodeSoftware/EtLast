﻿namespace FizzCode.EtLast.Diagnostics.Interface;

public class EventParser
{
    public static ProcessStartEvent ReadProcessStartEvent(ExtendedBinaryReader reader)
    {
        return new ProcessStartEvent
        {
            ProcessId = reader.Read7BitEncodedInt64(),
            Type = reader.ReadString(),
            Kind = reader.ReadNullableString(),
            Name = reader.ReadString(),
            CallerProcessId = reader.ReadNullable7BitEncodedInt64()
        };
    }

    public static ProcessEndEvent ReadProcessEndEvent(ExtendedBinaryReader reader)
    {
        return new ProcessEndEvent
        {
            ProcessId = reader.Read7BitEncodedInt64(),
            ElapsedMilliseconds = reader.Read7BitEncodedInt64(),
            NetTimeMilliseconds = reader.ReadNullable7BitEncodedInt64(),
        };
    }

    public IoCommandStartEvent ReadIoCommandStartEvent(ExtendedBinaryReader reader)
    {
        var evt = new IoCommandStartEvent
        {
            Id = reader.Read7BitEncodedInt64(),
            ProcessId = reader.Read7BitEncodedInt64(),
            Kind = (IoCommandKind)reader.ReadByte(),
            Location = reader.ReadNullableString(),
            Path = reader.ReadNullableString(),
            TimeoutSeconds = reader.ReadNullable7BitEncodedInt32(),
            Command = reader.ReadNullableString(),
            TransactionId = reader.ReadNullableString(),
        };

        var argCount = reader.Read7BitEncodedInt();
        if (argCount > 0)
        {
            evt.Arguments = new KeyValuePair<string, object>[argCount];
            for (var i = 0; i < argCount; i++)
            {
                var name = reader.ReadString();
                var value = reader.ReadObject();
                evt.Arguments[i] = new KeyValuePair<string, object>(name, value);
            }
        }

        return evt;
    }

    public static IoCommandEndEvent ReadIoCommandEndEvent(ExtendedBinaryReader reader)
    {
        var evt = new IoCommandEndEvent
        {
            Id = reader.Read7BitEncodedInt64(),
            AffectedDataCount = reader.ReadNullable7BitEncodedInt64(),
            ErrorMessage = reader.ReadNullableString(),
        };

        return evt;
    }

    public RowCreatedEvent ReadRowCreatedEvent(ExtendedBinaryReader reader)
    {
        var evt = new RowCreatedEvent
        {
            ProcessId = reader.Read7BitEncodedInt64(),
            RowId = reader.Read7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = reader.ReadString();
                var value = reader.ReadObject();
                evt.Values[i] = new KeyValuePair<string, object>(column, value);
            }
        }

        return evt;
    }

    public static RowOwnerChangedEvent ReadRowOwnerChangedEvent(ExtendedBinaryReader reader)
    {
        return new RowOwnerChangedEvent
        {
            RowId = reader.Read7BitEncodedInt64(),
            PreviousProcessId = reader.Read7BitEncodedInt64(),
            NewProcessId = reader.ReadNullable7BitEncodedInt64()
        };
    }

    public RowValueChangedEvent ReadRowValueChangedEvent(ExtendedBinaryReader reader)
    {
        var evt = new RowValueChangedEvent
        {
            RowId = reader.Read7BitEncodedInt64(),
            ProcessId = reader.ReadNullable7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = reader.ReadString();
                var value = reader.ReadObject();
                evt.Values[i] = new KeyValuePair<string, object>(column, value);
            }
        }

        return evt;
    }

    public SinkStartedEvent ReadSinkStartedEvent(ExtendedBinaryReader reader)
    {
        var evt = new SinkStartedEvent
        {
            Id = reader.Read7BitEncodedInt64(),
            Location = reader.ReadNullableString(),
            Path = reader.ReadNullableString(),
            Format = reader.ReadNullableString(),
            ProcessId = reader.Read7BitEncodedInt64(),
        };

        return evt;
    }

    public WriteToSinkEvent ReadWriteToSinkEvent(ExtendedBinaryReader reader)
    {
        var evt = new WriteToSinkEvent
        {
            RowId = reader.Read7BitEncodedInt64(),
            ProcessId = reader.Read7BitEncodedInt64(),
            SinkId = reader.Read7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = reader.ReadString();
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
            TransactionId = reader.ReadNullableString(),
            Text = reader.ReadString(),
            Severity = (LogSeverity)reader.ReadByte(),
            ProcessId = reader.ReadNullable7BitEncodedInt64()
        };

        var argCount = reader.Read7BitEncodedInt();
        if (argCount > 0)
        {
            evt.Arguments = new KeyValuePair<string, object>[argCount];
            for (var i = 0; i < argCount; i++)
            {
                var key = reader.ReadString();
                var value = reader.ReadObject();
                evt.Arguments[i] = new KeyValuePair<string, object>(key, value);
            }
        }

        return evt;
    }
}
