namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    internal enum ColorCode
    {
        LvlTokenVrb = 0,
        LvlTokenDbg = 1,
        LvlTokenInf = 2,
        LvlTokenWrn = 3,
        LvlTokenErr = 4,
        LvlTokenFtl = 5,
        Message_Exception = 6,
        TimeStamp_Property_Exception = 7,
        Value = 8,
        NullValue = 9,
        StructureName = 10,
        StringValue = 11,
        NumberValue = 12,
        BooleanValue = 13,
        ScalarValue = 14,
        TimeSpanValue = 15,
        Module = 16,
        Plugin = 17,
        Process = 18,
        Operation = 19,
        Job = 20,
    }
}