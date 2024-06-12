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
    _datetime = 9,
    _datetimeoffset = 10,
    _timespan = 11,
    _guid = 12,
    _bool = 13,
    _float = 14,
    _double = 15,
    _decimal = 16,
    _half = 17,
    _bytearray = 18,
    _char = 19,
    _uint128 = 20,
    _unknown = byte.MaxValue,
};