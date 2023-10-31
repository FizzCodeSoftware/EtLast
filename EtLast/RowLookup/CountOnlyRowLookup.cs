namespace FizzCode.EtLast;

public sealed class CountOnlyRowLookup : ICountableLookup
{
    /// <summary>
    /// Default false.
    /// </summary>
    public bool IgnoreCase { get; }

    public int Count { get; private set; }
    public IEnumerable<string> Keys => _dictionary.Keys;

    private readonly Dictionary<string, int> _dictionary;

    public CountOnlyRowLookup(bool ignoreCase = false)
    {
        IgnoreCase = ignoreCase;
        _dictionary = ignoreCase
            ? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, int>();
    }

    public void AddRow(string key, IReadOnlySlimRow row)
    {
        if (string.IsNullOrEmpty(key))
            return;

        Count++;
        _dictionary.TryGetValue(key, out var count);
        _dictionary[key] = count + 1;
    }

    public int CountByKey(string key)
    {
        if (key == null)
            return 0;

        _dictionary.TryGetValue(key, out var count);
        return count;
    }

    public void Clear()
    {
        _dictionary.Clear();
    }
}
