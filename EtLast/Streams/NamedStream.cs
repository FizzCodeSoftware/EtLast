namespace FizzCode.EtLast;

public class NamedStream(string name, Stream stream, IoCommand ioCommand)
{
    public string Name { get; } = name;
    public Stream Stream { get; private set; } = stream;
    public IoCommand IoCommand { get; } = ioCommand;
    public EventHandler OnDispose { get; set; }

    public long Rows { get; private set; }
    public long Characters { get; private set; }
    public long Bytes { get; private set; }

    private readonly Dictionary<string, string> _metadata = new(StringComparer.InvariantCultureIgnoreCase);
    public IEnumerable<KeyValuePair<string, string>> Metadata => new[]
    {
        new KeyValuePair<string, string>("Rows", Rows.ToString("D", CultureInfo.InvariantCulture)),
        new KeyValuePair<string, string>("Characters", Characters.ToString("D", CultureInfo.InvariantCulture)),
        new KeyValuePair<string, string>("Bytes", Bytes.ToString("D", CultureInfo.InvariantCulture)),
    }
    .Concat(_metadata);

    public void IncreaseRows(long count = 1) => Rows += count;
    public void IncreaseCharacters(long count = 1) => Characters += count;
    public void IncreaseBytes(long count = 1) => Bytes += count;

    public virtual void Close()
    {
        OnDispose?.Invoke(this, EventArgs.Empty);

        if (Stream != null)
        {
            Stream.Dispose();
            Stream = null;
        }
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