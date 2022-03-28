namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public class ContinuousAggregate
{
    public SlimRow ResultRow { get; }

    public ContinuousAggregate(object tag)
    {
        ResultRow = new SlimRow()
        {
            Tag = tag,
        };
    }

    public int RowsInGroup { get; set; }
    private Dictionary<string, object> _state;

    public T GetStateValue<T>(string uniqueName, T defaultValue)
    {
        if (_state == null)
            return defaultValue;

        if (_state.TryGetValue(uniqueName, out var v) && (v is T value))
            return value;

        return defaultValue;
    }

    public void SetStateValue<T>(string uniqueName, T value)
    {
        if (_state == null)
            _state = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        _state[uniqueName] = value;
    }
}
