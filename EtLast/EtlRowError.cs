namespace FizzCode.EtLast;

using System;
using System.Globalization;

public sealed class EtlRowError
{
    public object OriginalValue { get; set; }
    public IProcess Process { get; set; }
    public string Message { get; set; }

    public EtlRowError()
    {
    }

    public EtlRowError(object originalValue)
    {
        OriginalValue = originalValue;
    }

    public override string ToString()
    {
        return string.Format(CultureInfo.InvariantCulture, "error: {0}, value: {1}, process: {2}",
                Message,
                OriginalValue != null ? OriginalValue + " (" + OriginalValue.GetType().GetFriendlyTypeName() + ")" : "NULL",
                Process?.Name ?? "unknown process").Replace("\n", @"\n", StringComparison.InvariantCultureIgnoreCase);
    }
}
