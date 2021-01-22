namespace FizzCode.EtLast
{
    public class SimpleExcelWriterState : BaseExcelWriterState
    {
        public int LastRow { get; set; } = 1;
        public int LastCol { get; set; } = 1;
    }
}