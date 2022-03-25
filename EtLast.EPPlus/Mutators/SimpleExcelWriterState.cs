namespace FizzCode.EtLast;

public class SimpleExcelWriterState : BaseExcelWriterState
{
    public int NextRow { get; set; } = 1;
    public int NextCol { get; set; } = 1;
}
