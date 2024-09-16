using System.Drawing;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class BinaryTypeCodeEncoder
{
    private static Dictionary<Type, BinaryTypeCode> TypeMap { get; } = new()
    {
        [typeof(int)] = BinaryTypeCode._int,
        [typeof(uint)] = BinaryTypeCode._uint,
        [typeof(long)] = BinaryTypeCode._long,
        [typeof(ulong)] = BinaryTypeCode._ulong,
        [typeof(sbyte)] = BinaryTypeCode._sbyte,
        [typeof(byte)] = BinaryTypeCode._byte,
        [typeof(short)] = BinaryTypeCode._short,
        [typeof(ushort)] = BinaryTypeCode._ushort,
        [typeof(string)] = BinaryTypeCode._string,
        [typeof(DateTime)] = BinaryTypeCode._datetime,
        [typeof(DateTimeOffset)] = BinaryTypeCode._datetimeoffset,
        [typeof(TimeSpan)] = BinaryTypeCode._timespan,
        [typeof(Guid)] = BinaryTypeCode._guid,
        [typeof(bool)] = BinaryTypeCode._bool,
        [typeof(float)] = BinaryTypeCode._float,
        [typeof(double)] = BinaryTypeCode._double,
        [typeof(decimal)] = BinaryTypeCode._decimal,
        [typeof(Half)] = BinaryTypeCode._half,
        [typeof(byte[])] = BinaryTypeCode._bytearray,
        [typeof(char)] = BinaryTypeCode._char,
        [typeof(UInt128)] = BinaryTypeCode._uint128,
        [typeof(Int128)] = BinaryTypeCode._int128,
        [typeof(DateOnly)] = BinaryTypeCode._dateonly,
        [typeof(TimeOnly)] = BinaryTypeCode._timeonly,
        [typeof(System.Drawing.Color)] = BinaryTypeCode._color,
    };

    public static BinaryTypeCode GetTypeCode(Type type)
    {
        return TypeMap.TryGetValue(type, out var typeCode)
            ? typeCode
            : BinaryTypeCode._unknown;
    }

    public static void Write(BinaryWriter writer, object value, BinaryTypeCode typeCode)
    {
        switch (typeCode)
        {
            case BinaryTypeCode._int:
                writer.Write7BitEncodedInt((int)value);
                break;
            case BinaryTypeCode._uint:
                unchecked
                {
                    writer.Write7BitEncodedInt((int)(uint)value);
                }
                break;
            case BinaryTypeCode._long:
                writer.Write7BitEncodedInt64((long)value);
                break;
            case BinaryTypeCode._ulong:
                unchecked
                {
                    writer.Write7BitEncodedInt64((long)(ulong)value);
                }
                break;
            case BinaryTypeCode._sbyte:
                writer.Write((sbyte)value);
                break;
            case BinaryTypeCode._byte:
                writer.Write((byte)value);
                break;
            case BinaryTypeCode._short:
                writer.Write((short)value);
                break;
            case BinaryTypeCode._ushort:
                writer.Write((ushort)value);
                break;
            case BinaryTypeCode._string:
                writer.Write((string)value);
                break;
            case BinaryTypeCode._datetime:
                writer.Write7BitEncodedInt64(((DateTime)value).Ticks);
                break;
            case BinaryTypeCode._datetimeoffset:
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Ticks);
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Offset.Ticks);
                break;
            case BinaryTypeCode._timespan:
                writer.Write7BitEncodedInt64(((TimeSpan)value).Ticks);
                break;
            case BinaryTypeCode._guid:
                writer.Write(((Guid)value).ToByteArray());
                break;
            case BinaryTypeCode._bool:
                writer.Write((bool)value);
                break;
            case BinaryTypeCode._float:
                writer.Write((float)value);
                break;
            case BinaryTypeCode._double:
                writer.Write((double)value);
                break;
            case BinaryTypeCode._decimal:
                writer.Write((decimal)value);
                break;
            case BinaryTypeCode._half:
                writer.Write((Half)value);
                break;
            case BinaryTypeCode._bytearray:
                var bytes = (byte[])value;
                writer.Write7BitEncodedInt(bytes.Length);
                writer.Write(bytes);
                break;
            case BinaryTypeCode._char:
                writer.Write((char)value);
                break;
            case BinaryTypeCode._uint128:
                var uint128 = (UInt128)value;
                writer.Write((ulong)(uint128 >> 64)); // upper
                writer.Write((ulong)uint128); // lower
                break;
            case BinaryTypeCode._int128:
                var int128 = (Int128)value;
                writer.Write((ulong)(int128 >> 64)); // upper
                writer.Write((ulong)int128); // lower
                break;
            case BinaryTypeCode._dateonly:
                writer.Write(((DateOnly)value).DayNumber);
                break;
            case BinaryTypeCode._timeonly:
                writer.Write(((TimeOnly)value).Ticks);
                break;
            case BinaryTypeCode._color:
                writer.Write(((System.Drawing.Color)value).ToArgb());
                break;
        }
    }

    public static object Read(BinaryReader reader, BinaryTypeCode typeCode)
    {
        switch (typeCode)
        {
            case BinaryTypeCode._int:
                return reader.Read7BitEncodedInt();
            case BinaryTypeCode._uint:
                unchecked
                {
                    return (uint)reader.Read7BitEncodedInt();
                }
            case BinaryTypeCode._long:
                return reader.Read7BitEncodedInt64();
            case BinaryTypeCode._ulong:
                unchecked
                {
                    return (ulong)reader.Read7BitEncodedInt64();
                }
            case BinaryTypeCode._sbyte:
                return reader.ReadSByte();
            case BinaryTypeCode._byte:
                return reader.ReadByte();
            case BinaryTypeCode._short:
                return reader.ReadInt16();
            case BinaryTypeCode._ushort:
                return reader.ReadUInt16();
            case BinaryTypeCode._string:
                return reader.ReadString();
            case BinaryTypeCode._datetime:
                return new DateTime(reader.Read7BitEncodedInt64());
            case BinaryTypeCode._datetimeoffset:
                return new DateTimeOffset(reader.Read7BitEncodedInt64(), new TimeSpan(reader.Read7BitEncodedInt64()));
            case BinaryTypeCode._timespan:
                return new TimeSpan(reader.Read7BitEncodedInt64());
            case BinaryTypeCode._guid:
                return new Guid(reader.ReadBytes(16));
            case BinaryTypeCode._bool:
                return reader.ReadBoolean();
            case BinaryTypeCode._float:
                return reader.ReadSingle();
            case BinaryTypeCode._double:
                return reader.ReadDouble();
            case BinaryTypeCode._decimal:
                return reader.ReadDecimal();
            case BinaryTypeCode._half:
                return reader.ReadHalf();
            case BinaryTypeCode._bytearray:
                var byteArrayLength = reader.Read7BitEncodedInt();
                return reader.ReadBytes(byteArrayLength);
            case BinaryTypeCode._char:
                return reader.ReadChar();
            case BinaryTypeCode._uint128:
                return new UInt128(reader.ReadUInt64(), reader.ReadUInt64()); // upper, lower
            case BinaryTypeCode._int128:
                return new Int128(reader.ReadUInt64(), reader.ReadUInt64()); // upper, lower
            case BinaryTypeCode._dateonly:
                return DateOnly.FromDayNumber(reader.ReadInt32());
            case BinaryTypeCode._timeonly:
                return new TimeOnly(reader.ReadInt64());
            case BinaryTypeCode._color:
                return Color.FromArgb(reader.ReadInt32());
        }

        return null;
    }
}