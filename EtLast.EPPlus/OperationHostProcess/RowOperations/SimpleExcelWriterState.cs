namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;

    public class SimpleExcelWriterState
    {
        public ExcelWorksheet LastWorksheet { get; set; }
        public int LastRow { get; set; } = 1;
        public int LastCol { get; set; } = 1;
    }
}