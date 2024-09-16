namespace FizzCode.EtLast;

public class DelimitedColumn
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public DelimitedColumn FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public IValueFormatter CustomFormatter { get; set; }

    public DelimitedColumn UseCustomFormatter(IValueFormatter formatter)
    {
        CustomFormatter = formatter;
        return this;
    }
}