namespace FizzCode.EtLast;

public class Sink
{
    public long Id { get; internal init; }
    public IEtlContext Context { get; internal init; }

    public string Location { get; internal init; }
    public string Path { get; internal init; }
    public string Format { get; internal init; }
    public long ProcessId { get; internal init; }

    public string[] Columns { get; internal init; }

    public long Rows { get; private set; }
    public long Characters { get; private set; }
    public long Bytes { get; private set; }

    public void IncreaseRows(long count = 1) => Rows += count;
    public void IncreaseCharacters(long count = 1) => Characters += count;
    public void IncreaseBytes(long count = 1) => Bytes += count;

    private readonly Dictionary<string, string> _metadata = new(StringComparer.InvariantCultureIgnoreCase);
    public IEnumerable<KeyValuePair<string, string>> Metadata => new[]
    {
        new KeyValuePair<string, string>("Rows", Rows.ToString("D", CultureInfo.InvariantCulture)),
        new KeyValuePair<string, string>("Characters", Characters.ToString("D", CultureInfo.InvariantCulture)),
        new KeyValuePair<string, string>("Bytes", Bytes.ToString("D", CultureInfo.InvariantCulture)),
    }
    .Concat(_metadata);

    public void RegisterRow(IReadOnlyRow row)
    {
        Rows++;

        foreach (var listener in Context.Listeners)
            listener.OnWriteToSink(this, row);
    }

    public void SetMetadata(string key, string value)
    {
        _metadata[key] = value;
    }

    public void SetMetadata(string key, int value)
    {
        _metadata[key] = value.ToString("D", CultureInfo.InvariantCulture);
    }

    public void SetMetadata(string key, long value)
    {
        _metadata[key] = value.ToString("D", CultureInfo.InvariantCulture);
    }
}