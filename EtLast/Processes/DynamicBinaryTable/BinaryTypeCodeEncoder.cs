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
    };

    public static BinaryTypeCode GetTypeCode(Type type)
    {
        return TypeMap.TryGetValue(type, out var typeCode)
            ? typeCode
            : BinaryTypeCode._unknown;
    }

    public static void EncodeByTypeCode(BinaryWriter writer, object value, BinaryTypeCode typeCode)
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
                unchecked
                {
                    writer.Write((sbyte)value);
                }
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
                writer.Write((byte[])value);
                break;
            case BinaryTypeCode._char:
                writer.Write((char)value);
                break;
            case BinaryTypeCode._uint128:
                writer.Write((ulong)(UInt128)value);
                writer.Write((ulong)(UInt128)value >> 64);
                break;
        }
    }
}