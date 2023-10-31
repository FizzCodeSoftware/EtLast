namespace FizzCode.EtLast.Diagnostics.Interface;

public class EventParser
{
    private readonly Dictionary<int, string> _textDictionary;

    public EventParser()
    {
        _textDictionary = new Dictionary<int, string>()
        {
            [0] = null,
        };
    }

    public void AddText(int id, string text)
    {
        if (text != null)
            text = string.Intern(text);

        _textDictionary[id] = text;
    }

    public string GetTextById(int id)
    {
        _textDictionary.TryGetValue(id, out var value);
        return value;
    }

    public static ProcessInvocationStartEvent ReadProcessInvocationStartEvent(ExtendedBinaryReader reader)
    {
        return new ProcessInvocationStartEvent
        {
            InvocationUID = reader.Read7BitEncodedInt64(),
            InstanceUID = reader.Read7BitEncodedInt64(),
            InvocationCounter = reader.Read7BitEncodedInt64(),
            Type = reader.ReadString(),
            Kind = reader.ReadNullableString(),
            Name = reader.ReadString(),
            Topic = reader.ReadNullableString(),
            CallerInvocationUID = reader.ReadNullable7BitEncodedInt64()
        };
    }

    public static ProcessInvocationEndEvent ReadProcessInvocationEndEvent(ExtendedBinaryReader reader)
    {
        return new ProcessInvocationEndEvent
        {
            InvocationUID = reader.Read7BitEncodedInt64(),
            ElapsedMilliseconds = reader.Read7BitEncodedInt64(),
            NetTimeMilliseconds = reader.ReadNullable7BitEncodedInt64(),
        };
    }

    public IoCommandStartEvent ReadIoCommandStartEvent(ExtendedBinaryReader reader)
    {
        var evt = new IoCommandStartEvent
        {
            Uid = reader.Read7BitEncodedInt64(),
            ProcessInvocationUid = reader.Read7BitEncodedInt64(),
            Kind = (IoCommandKind)reader.ReadByte(),
            Location = GetTextById(reader.Read7BitEncodedInt()),
            Path = GetTextById(reader.Read7BitEncodedInt()),
            TimeoutSeconds = reader.ReadNullable7BitEncodedInt32(),
            Command = reader.ReadNullableString(),
            TransactionId = GetTextById(reader.Read7BitEncodedInt()),
        };

        var argCount = reader.Read7BitEncodedInt();
        if (argCount > 0)
        {
            evt.Arguments = new KeyValuePair<string, object>[argCount];
            for (var i = 0; i < argCount; i++)
            {
                var name = GetTextById(reader.Read7BitEncodedInt());
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
            Uid = reader.Read7BitEncodedInt64(),
            AffectedDataCount = reader.ReadNullable7BitEncodedInt64(),
            ErrorMessage = reader.ReadNullableString(),
        };

        return evt;
    }

    public RowCreatedEvent ReadRowCreatedEvent(ExtendedBinaryReader reader)
    {
        var evt = new RowCreatedEvent
        {
            ProcessInvocationUid = reader.Read7BitEncodedInt64(),
            RowUid = reader.Read7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = GetTextById(reader.Read7BitEncodedInt());
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
            RowUid = reader.Read7BitEncodedInt64(),
            PreviousProcessInvocationUid = reader.Read7BitEncodedInt64(),
            NewProcessInvocationUid = reader.ReadNullable7BitEncodedInt64()
        };
    }

    public RowValueChangedEvent ReadRowValueChangedEvent(ExtendedBinaryReader reader)
    {
        var evt = new RowValueChangedEvent
        {
            RowUid = reader.Read7BitEncodedInt64(),
            ProcessInvocationUID = reader.ReadNullable7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = GetTextById(reader.Read7BitEncodedInt());
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
            UID = reader.Read7BitEncodedInt64(),
            Location = GetTextById(reader.Read7BitEncodedInt()),
            Path = GetTextById(reader.Read7BitEncodedInt()),
        };

        return evt;
    }

    public WriteToSinkEvent ReadWriteToSinkEvent(ExtendedBinaryReader reader)
    {
        var evt = new WriteToSinkEvent
        {
            RowUid = reader.Read7BitEncodedInt64(),
            ProcessInvocationUID = reader.Read7BitEncodedInt64(),
            SinkUID = reader.Read7BitEncodedInt64()
        };

        var columnCount = reader.Read7BitEncodedInt();
        if (columnCount > 0)
        {
            evt.Values = new KeyValuePair<string, object>[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                var column = GetTextById(reader.Read7BitEncodedInt());
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
            TransactionId = GetTextById(reader.Read7BitEncodedInt()),
            Text = reader.ReadString(),
            Severity = (LogSeverity)reader.ReadByte(),
            ProcessInvocationUID = reader.ReadNullable7BitEncodedInt64()
        };

        var argCount = reader.Read7BitEncodedInt();
        if (argCount > 0)
        {
            evt.Arguments = new KeyValuePair<string, object>[argCount];
            for (var i = 0; i < argCount; i++)
            {
                var key = GetTextById(reader.Read7BitEncodedInt());
                var value = reader.ReadObject();
                evt.Arguments[i] = new KeyValuePair<string, object>(key, value);
            }
        }

        return evt;
    }
}
