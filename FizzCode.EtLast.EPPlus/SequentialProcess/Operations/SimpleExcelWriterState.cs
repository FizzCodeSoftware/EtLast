namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;

    public class SimpleExcelWriterState
    {
        public ExcelWorksheet LastWorksheet { get; set; }
        public int LastRow { get; set; }
        public int LastCol { get; set; }
    }
}