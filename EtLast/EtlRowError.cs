namespace FizzCode.EtLast
{
    using System;

    public class EtlRowError
    {
        public object OriginalValue { get; set; }
        public IProcess Process { get; set; }
        public IRowOperation Operation { get; set; }
        public string Message { get; set; }

        public override string ToString()
        {
            return Operation != null
                ? string.Format("{0}\nvalue: {1}\nprocess: {2}\noperation: {3}",
                    Message,
                    OriginalValue != null ? OriginalValue + " (" + TypeHelpers.GetFriendlyTypeName(OriginalValue.GetType()) + ")" : "NULL",
                    Process?.Name ?? "unknown process",
                    Operation?.Name ?? "unknown operation").Replace("\n", Environment.NewLine)
                : string.Format("{0}\nvalue: {1}\nprocess: {2}",
                    Message,
                    OriginalValue != null ? OriginalValue + " (" + TypeHelpers.GetFriendlyTypeName(OriginalValue.GetType()) + ")" : "NULL",
                    Process?.Name ?? "unknown process").Replace("\n", Environment.NewLine);
        }
    }
}