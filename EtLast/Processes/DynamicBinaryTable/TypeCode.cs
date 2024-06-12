namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public enum TypeCode : byte
{
    _int = 0,
    _uint = 1,
    _long = 2,
    _ulong = 3,
    _sbyte = 4,
    _byte = 5,
    _short = 6,
    _ushort = 7,
    _string = 8,
    _DateTime = 9,
    _DateTimeOffset = 10,
    _TimeSpan = 11,
    _Guid = 12,
    _bool = 13,
    _float = 14,
    _double = 15,
    _decimal = 16,
    _Half = 17,
    _byteArray = 18,
    _char = 19,
    _UInt128 = 20,
    _unknown = byte.MaxValue,
};