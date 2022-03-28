namespace FizzCode.EtLast;

public class ExcelColumnConfiguration
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public ExcelColumnConfiguration FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string NumberFormat { get; private set; }

    public ExcelColumnConfiguration SetNumberFormat(string format)
    {
        NumberFormat = format;
        return this;
    }
}
