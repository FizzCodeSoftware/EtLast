﻿namespace FizzCode.EtLast.Diagnostics.Interface;

public class WriteToSinkEvent : AbstractRowEvent
{
    public long ProcessId { get; set; }
    public long SinkId { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
