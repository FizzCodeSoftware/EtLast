namespace FizzCode.EtLast;

public class Sink
{
    public long Id { get; set; }

    public string Location { get; init; }
    public string Path { get; init; }
    public string Format { get; init; }
    public Type WriterType { get; init; }

    public int RowsWritten { get; set; }
}