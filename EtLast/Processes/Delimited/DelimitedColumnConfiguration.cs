namespace FizzCode.EtLast;

public class DelimitedColumnConfiguration
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public DelimitedColumnConfiguration FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public IValueFormatter CustomFormatter { get; set; }

    public DelimitedColumnConfiguration UseCustomFormatter(IValueFormatter formatter)
    {
        CustomFormatter = formatter;
        return this;
    }
}
