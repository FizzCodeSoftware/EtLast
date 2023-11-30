namespace FizzCode.EtLast;

public class IoCommand
{
    public long Id { get; set; }

    public required IoCommandKind Kind { get; init; }
    public int? TimeoutSeconds { get; init; }
    public string Location { get; init; }
    public string Command { get; init; }
    public string Path { get; init; }
    public string TransactionId { get; init; }

    public Func<IEnumerable<KeyValuePair<string, object>>> ArgumentListGetter { get; init; }
    public string Message { get; init; }
    public string MessageExtra { get; init; }

    public long? AffectedDataCount { get; set; }
    public Exception Exception { get; set; }
}
