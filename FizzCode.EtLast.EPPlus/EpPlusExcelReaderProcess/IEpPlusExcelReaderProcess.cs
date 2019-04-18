namespace FizzCode.EtLast.EPPlus
{
    using System.Collections.Generic;

    public enum EpPlusExcelHeaderCellMode { Join, KeepFirst, KeepLast }

    public interface IEpPlusExcelReaderProcess : IProcess
    {
        IProcess InputProcess { get; set; }

        string FileName { get; set; }
        string SheetName { get; set; }
        int SheetIndex { get; set; }
        string AddRowIndexToColumn { get; set; }
        bool TreatEmptyStringAsNull { get; set; }
        List<(string ExcelColumn, string RowColumn, ITypeConverter Converter, object ValueIfNull)> ColumnMap { get; set; }
        bool Transpose { get; set; }

        EpPlusExcelHeaderCellMode HeaderCellMode { get; set; }
        string HeaderRowJoinSeparator { get; set; }

        int[] HeaderRows { get; set; }
        int FirstDataRow { get; set; }
        int FirstDataColumn { get; set; }
    }
}