namespace FizzCode.EtLast
{
    using OfficeOpenXml;

    public class BaseExcelWriterState
    {
        public ExcelWorksheet LastWorksheet { get; internal set; }
    }
}