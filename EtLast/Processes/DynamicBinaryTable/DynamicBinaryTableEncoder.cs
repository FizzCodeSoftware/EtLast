namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DynamicBinaryTableEncoder
{
    public static Dictionary<Type, int> TypeMap { get; } = new()
    {
        [typeof(int)] = 0,
        [typeof(uint)] = 1,
        [typeof(long)] = 2,
        [typeof(ulong)] = 3,
        [typeof(sbyte)] = 4,
        [typeof(byte)] = 5,
        [typeof(short)] = 6,
        [typeof(ushort)] = 7,
        [typeof(string)] = 8,
        [typeof(DateTime)] = 9,
        [typeof(DateTimeOffset)] = 10,
        [typeof(TimeSpan)] = 11,
        [typeof(Guid)] = 12,
        [typeof(bool)] = 13,
        [typeof(float)] = 14,
        [typeof(double)] = 15,
        [typeof(decimal)] = 16,
        [typeof(Half)] = 17,
        [typeof(byte[])] = 18,
        [typeof(char)] = 19,
        [typeof(UInt128)] = 20,
    };

    public static int GetTypeCode(Type type)
    {
        return TypeMap.TryGetValue(type, out var typeCode)
            ? typeCode
            : -1;
    }

    public static void EncodeByTypeCode(BinaryWriter writer, object value, int typeCode)
    {
        switch (typeCode)
        {
            case 0: // int
                writer.Write7BitEncodedInt((int)value);
                break;
            case 1: // uint
                unchecked
                { writer.Write7BitEncodedInt((int)(uint)value); }
                break;
            case 2: // long
                writer.Write7BitEncodedInt64((long)value);
                break;
            case 3: // ulong
                unchecked
                { writer.Write7BitEncodedInt64((long)(ulong)value); }
                break;
            case 4: // sbyte
                unchecked
                { writer.Write((sbyte)value); }
                break;
            case 5: // byte
                writer.Write((byte)value);
                break;
            case 6: // short
                writer.Write((short)value);
                break;
            case 7: // ushort
                writer.Write((ushort)value);
                break;
            case 8: // string
                writer.Write((string)value);
                break;
            case 9: // DateTime
                writer.Write7BitEncodedInt64(((DateTime)value).Ticks);
                break;
            case 10: // DateTimeOffset
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Ticks);
                writer.Write7BitEncodedInt64(((DateTimeOffset)value).Offset.Ticks);
                break;
            case 11: // TimeSpan
                writer.Write7BitEncodedInt64(((TimeSpan)value).Ticks);
                break;
            case 12: // Guid
                writer.Write(((Guid)value).ToByteArray());
                break;
            case 13: // bool
                writer.Write((bool)value);
                break;
            case 14: // float
                writer.Write((float)value);
                break;
            case 15: // double
                writer.Write((double)value);
                break;
            case 16: // decimal
                writer.Write((decimal)value);
                break;
            case 17: // Half
                writer.Write((Half)value);
                break;
            case 18: // byte[]
                writer.Write((byte[])value);
                break;
            case 19: // char
                writer.Write((char)value);
                break;
            case 20: // UInt128
                writer.Write((ulong)(UInt128)value);
                writer.Write((ulong)(UInt128)value >> 64);
                break;
        }
    }
}