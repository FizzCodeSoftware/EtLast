namespace FizzCode.EtLast;

public class ExcelColumn
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string SourceColumn { get; private set; }

    public ExcelColumn FromSource(string sourceColumn)
    {
        SourceColumn = sourceColumn;
        return this;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public string NumberFormat { get; private set; }

    public ExcelColumn SetNumberFormat(string format)
    {
        NumberFormat = format;
        return this;
    }
}
