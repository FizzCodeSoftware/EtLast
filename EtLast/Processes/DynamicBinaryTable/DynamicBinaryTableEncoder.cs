namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DynamicBinaryTableEncoder
{
    private static Dictionary<Type, TypeCode> TypeMap { get; } = new()
    {
        [typeof(int)] = TypeCode._int,
        [typeof(uint)] = TypeCode._uint,
        [typeof(long)] = TypeCode._long,
        [typeof(ulong)] = TypeCode._ulong,
        [typeof(sbyte)] = TypeCode._sbyte,
        [typeof(byte)] = TypeCode._byte,
        [typeof(short)] = TypeCode._short,
        [typeof(ushort)] = TypeCode._ushort,
        [typeof(string)] = TypeCode._string,
        [typeof(DateTime)] = TypeCode._DateTime,
        [typeof(DateTimeOffset)] = TypeCode._DateTimeOffset,
        [typeof(TimeSpan)] = TypeCode._TimeSpan,
        [typeof(Guid)] = TypeCode._Guid,
        [typeof(bool)] = TypeCode._bool,
        [typeof(float)] = TypeCode._float,
        [typeof(double)] = TypeCode._double,
        [typeof(decimal)] = TypeCode._decimal,
        [typeof(Half)] = TypeCode._Half,
        [typeof(byte[])] = TypeCode._byteArray,
        [typeof(char)] = TypeCode._char,
        [typeof(UInt128)] = TypeCode._UInt128,
    };

    public static TypeCode GetTypeCode(Type type)
    {
        return TypeMap.TryGetValue(type, out var typeCode)
            ? typeCode
            : TypeCode._unknown;
    }

    public static void EncodeByTypeCode(BinaryWriter writer, object value, TypeCode typeCode)
    {
        switch (typeCode)
        {
            case TypeCode._int:
                writer.Write7BitEncodedInt((int)value);
                break;
            case TypeCode._uint:
                unchecked
                {
                    writer.Write7BitEncodedInt((int)(uint)value);
                }
                break;
            case TypeCode._long:
                writer.Write7BitEncodedInt64((long)value);
                break;
            case TypeCode._ulong:
                unchecked
                {
                    writer.Write7BitEncodedInt64((long)(ulong)value);
                }
                break;
            case TypeCode._sbyte:
                unchecked
                {
                    writer.Write((sbyte)value);
                }
                break;
            case TypeCode._byte:
                writer.Write((byte)value);
                break;
            case TypeCode._short:
                writer.Write((short)value);
                break;
            case TypeCode._ushort:
                writer.Write((ushort)value);
                break;
            case TypeCode._string:
                writer.Write((string)value);
                break;
            case TypeCode._DateTime:
                writer.Write7BitEncodedInt64(((DateTime)value).Ticks);
                break;
            case TypeCode._DateTimeOffset:
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Ticks);
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Offset.Ticks);
                break;
            case TypeCode._TimeSpan:
                writer.Write7BitEncodedInt64(((TimeSpan)value).Ticks);
                break;
            case TypeCode._Guid:
                writer.Write(((Guid)value).ToByteArray());
                break;
            case TypeCode._bool:
                writer.Write((bool)value);
                break;
            case TypeCode._float:
                writer.Write((float)value);
                break;
            case TypeCode._double:
                writer.Write((double)value);
                break;
            case TypeCode._decimal:
                writer.Write((decimal)value);
                break;
            case TypeCode._Half:
                writer.Write((Half)value);
                break;
            case TypeCode._byteArray:
                writer.Write((byte[])value);
                break;
            case TypeCode._char:
                writer.Write((char)value);
                break;
            case TypeCode._UInt128:
                writer.Write((ulong)(UInt128)value);
                writer.Write((ulong)(UInt128)value >> 64);
                break;
        }
    }
}