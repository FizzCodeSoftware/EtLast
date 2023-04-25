namespace FizzCode.EtLast;

public sealed class EtlRowError
{
    public object OriginalValue { get; }
    public IProcess Process { get; }
    public string Message { get; }

    public EtlRowError(object originalValue)
    {
        OriginalValue = originalValue;
    }

    public EtlRowError(IProcess process, object originalValue, string message)
    {
        Process = process;
        OriginalValue = originalValue;
        Message = message;
    }

    public EtlRowError(IProcess process, object originalValue, Exception ex)
    {
        Process = process;
        OriginalValue = originalValue;
        Message = ex.Message;
    }

    public override string ToString()
    {
        return string.Format(CultureInfo.InvariantCulture, "error: {0}, value: {1}, process: {2}",
                Message,
                OriginalValue != null ? OriginalValue + " (" + OriginalValue.GetType().GetFriendlyTypeName() + ")" : "NULL",
                Process?.Name ?? "unknown process").Replace("\n", @"\n", StringComparison.InvariantCultureIgnoreCase);
    }
}