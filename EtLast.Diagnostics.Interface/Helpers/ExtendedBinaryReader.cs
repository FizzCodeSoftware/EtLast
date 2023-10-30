namespace FizzCode.EtLast.Diagnostics.Interface;

public class ExtendedBinaryReader : BinaryReader
{
    public ExtendedBinaryReader(Stream input, Encoding encoding)
        : base(input, encoding)
    {
    }

    public string ReadNullableString()
    {
        if (!ReadBoolean())
            return null;

        return ReadString();
    }

    public int? ReadNullableInt32()
    {
        if (!ReadBoolean())
            return null;

        return Read7BitEncodedInt();
    }

    public long? ReadNullableInt64()
    {
        if (!ReadBoolean())
            return null;

        return ReadInt64();
    }

    public object ReadObject()
    {
        var hasValue = ReadBoolean();
        if (!hasValue)
            return null;

        var type = (ArgumentType)ReadByte();

        switch (type)
        {
            case ArgumentType._error:
                return new EtlRowError(ReadString());
            case ArgumentType._removed:
                return new EtlRowRemovedValue();
            case ArgumentType._string:
                return ReadString();
            case ArgumentType._stringArray:
                var count = Read7BitEncodedInt();
                var strArr = new string[count];
                for (var i = 0; i < count; i++)
                {
                    strArr[i] = ReadNullableString();
                }
                return strArr;
            case ArgumentType._bool:
                return ReadBoolean();
            case ArgumentType._char:
                return ReadChar();
            case ArgumentType._sbyte:
                return ReadSByte();
            case ArgumentType._byte:
                return ReadByte();
            case ArgumentType._short:
                return ReadInt16();
            case ArgumentType._ushort:
            case ArgumentType._int:
                return Read7BitEncodedInt();
            case ArgumentType._uint:
                return ReadUInt32();
            case ArgumentType._long:
                return ReadInt64();
            case ArgumentType._ulong:
                return ReadUInt64();
            case ArgumentType._float:
                return ReadSingle();
            case ArgumentType._double:
                return ReadDouble();
            case ArgumentType._decimal:
                return ReadDecimal();
            case ArgumentType._datetime:
                return new DateTime(ReadInt64());
            case ArgumentType._datetimeoffset:
                var ticks = ReadInt64();
                var offsetTicks = ReadInt64();
                return new DateTimeOffset(ticks, new TimeSpan(offsetTicks));
            case ArgumentType._timespan:
                return TimeSpan.FromMilliseconds(ReadDouble());
            default:
                throw new NotSupportedException(nameof(ArgumentType) + "." + type.ToString());
        }
    }
}
