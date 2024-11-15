﻿namespace FizzCode.EtLast;

internal enum ColorCode
{
    LvlTokenVrb = 0,
    LvlTokenDbg = 1,
    LvlTokenInf = 2,
    LvlTokenWrn = 3,
    LvlTokenErr = 4,
    LvlTokenFtl = 5,
    Exception = 6,
    TimeStamp_Property_Exception = 7,
    Value = 8,
    NullValue = 9,
    StructureName = 10,
    StringValue = 11,
    NumberValue = 12,
    BooleanValue = 13,
    ScalarValue = 14,
    TimeSpanValue = 15,
    UNUSED = 18, // previously used for Topic
    Process = 19,
    Operation = 20,
    Job = 21,
    ConnectionStringName = 22,
    Location = 23,
    Transaction = 24,
    Task = 25,
    Result = 26,
    ResultFailed = 27,
}