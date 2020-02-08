namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;

    public class EtlRowError
    {
        public object OriginalValue { get; set; }
        public IProcess Process { get; set; }
        public string Message { get; set; }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}\nvalue: {1}\nprocess: {2}",
                    Message,
                    OriginalValue != null ? OriginalValue + " (" + OriginalValue.GetType().GetFriendlyTypeName() + ")" : "NULL",
                    Process?.Name ?? "unknown process").Replace("\n", Environment.NewLine, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}