namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;

    public class EpPlusExcelReaderProcess : IEpPlusExcelReaderProcess
    {
        public IEtlContext Context { get; }
        public string Name { get; }

        public IProcess Caller { get; private set; }
        public IProcess InputProcess { get; set; }

        public string FileName { get; set; }
        public string SheetName { get; set; }
        public int SheetIndex { get; set; } = -1;

        /// <summary>
        /// First row index is (integer) 1
        /// </summary>
        public string AddRowIndexToColumn { get; set; }

        public bool TreatEmptyStringAsNull { get; set; }
        public bool IgnoreNullOrEmptyRows { get; set; } = true;

        public List<(string ExcelColumn, string RowColumn, ITypeConverter Converter, object ValueIfNull)> ColumnMap { get; set; }
        public ITypeConverter DefaultConverter { get; set; }
        public object DefaultValueIfNull { get; set; }

        public bool Transpose { get; set; } = false;

        public int[] HeaderRows { get; set; } = new[] { 1 };

        /// <summary>
        /// Default value is <see cref="EpPlusExcelHeaderCellMode.KeepLast"/>
        /// </summary>
        public EpPlusExcelHeaderCellMode HeaderCellMode { get; set; } = EpPlusExcelHeaderCellMode.KeepLast;

        /// <summary>
        /// Default value is "/"
        /// </summary>
        public string HeaderRowJoinSeparator { get; set; } = "/";

        public int FirstDataRow { get; set; } = 2;
        public int FirstDataColumn { get; set; } = 1;

        public EpPlusExcelReaderProcess(IEtlContext context, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName)) throw new ProcessParameterNullException(this, nameof(FileName));
            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1) throw new ProcessParameterNullException(this, nameof(SheetName));
            if (ColumnMap == null) throw new ProcessParameterNullException(this, nameof(ColumnMap));

            var sw = Stopwatch.StartNew();

            var index = 0;
            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

                var inputRows = InputProcess.Evaluate(this);
                var rowCount = 0;
                foreach (var row in inputRows)
                {
                    rowCount++;
                    index++;
                    if (AddRowIndexToColumn != null) row.SetValue(AddRowIndexToColumn, index, this);
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", rowCount, InputProcess.Name, sw.Elapsed);
            }

            if (!File.Exists(FileName))
            {
                var exception = new EtlException(this, "excel file doesn't exists");
                exception.AddOpsMessage(string.Format("excel file doesn't exists, file name: {0}", FileName));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            var resultCount = 0;
            if (!string.IsNullOrEmpty(SheetName)) Context.Log(LogSeverity.Information, this, "reading from {FileName}/{SheetName}", FileName, SheetName);
            else Context.Log(LogSeverity.Information, this, "reading from {FileName}/#{SheetIndex}", FileName, SheetIndex);

            if (Transpose)
            {
                throw new NotImplementedException("Transpose is not finished yet, must be tested before used");
            }

            var columnIndexes = new Dictionary<string, (int Index, ITypeConverter Converter, object ValueIfNull)>();

            ExcelPackage package = null;
            try
            {
                package = new ExcelPackage(new FileInfo(FileName));
            }
            catch (Exception ex)
            {
                var exception = new EtlException(this, "excel file read failed", ex);
                exception.AddOpsMessage(string.Format("excel file read failed, file name: {0}, message {1}", FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            using (package)
            {
                package.Compatibility.IsWorksheets1Based = false;
                var workbook = package.Workbook;
                if (workbook == null || workbook.Worksheets.Count == 0) yield break;
                var sheet = !string.IsNullOrEmpty(SheetName) ? workbook.Worksheets[SheetName] : workbook.Worksheets[SheetIndex];
                if (sheet == null)
                {
                    if (!string.IsNullOrEmpty(SheetName))
                    {
                        var exception = new EtlException(this, "can't read excel sheet");
                        exception.AddOpsMessage(string.Format("can't read excel sheet, file name: {0}, sheet: {1}, existing sheet names: {2}", FileName, SheetName, string.Join(",", workbook.Worksheets.Select(x => x.Name))));
                        exception.Data.Add("FileName", FileName);
                        exception.Data.Add("SheetName", SheetName);
                        exception.Data.Add("ExistingSheetNames", string.Join(",", workbook.Worksheets.Select(x => x.Name)));
                        throw exception;
                    }

                    yield break;
                }

                var rowCount = sheet.Dimension.End.Row;

                var endColumn = !Transpose ? sheet.Dimension.End.Column : sheet.Dimension.End.Row;
                var endRow = !Transpose ? sheet.Dimension.End.Row : sheet.Dimension.End.Column;

                for (int colIndex = FirstDataColumn; colIndex <= endColumn; colIndex++)
                {
                    var excelColumn = string.Empty;

                    if (!Transpose)
                    {
                        for (var headerRowIndex = 0; headerRowIndex < HeaderRows.Length; headerRowIndex++)
                        {
                            var ri = HeaderRows[headerRowIndex];

                            var c = sheet.Cells[ri, colIndex].Value?.ToString();
                            if (!string.IsNullOrEmpty(c))
                            {
                                if (HeaderCellMode == EpPlusExcelHeaderCellMode.Join)
                                {
                                    excelColumn += (!string.IsNullOrEmpty(excelColumn) ? HeaderRowJoinSeparator : string.Empty) + c;
                                }
                                else if (HeaderCellMode == EpPlusExcelHeaderCellMode.KeepFirst)
                                {
                                    if (string.IsNullOrEmpty(excelColumn)) excelColumn = c;
                                }
                                else excelColumn = c;
                            }
                        }
                    }
                    else
                    {
                        // support transpose here...
                    }

                    if (string.IsNullOrEmpty(excelColumn)) continue;

                    var givenColumn = ColumnMap.FirstOrDefault(x => string.Compare(x.ExcelColumn, excelColumn, true) == 0);
                    if (givenColumn.RowColumn != null)
                    {
                        columnIndexes.Add(givenColumn.RowColumn, (colIndex, givenColumn.Converter, givenColumn.ValueIfNull));
                    }
                    else if (DefaultConverter != null)
                    {
                        columnIndexes.Add(excelColumn, (colIndex, DefaultConverter, DefaultValueIfNull));
                    }
                }

                for (int rowIndex = FirstDataRow; rowIndex <= endRow; rowIndex++)
                {
                    var row = Context.CreateRow(columnIndexes.Count);
                    foreach (var kvp in columnIndexes)
                    {
                        var ri = !Transpose ? rowIndex : kvp.Value.Index;
                        var ci = !Transpose ? kvp.Value.Index : rowIndex;

                        var value = sheet.Cells[ri, ci].Value;
                        if (value != null && TreatEmptyStringAsNull && (value is string str) && str == string.Empty)
                        {
                            value = null;
                        }

                        if (value != null && kvp.Value.Converter != null)
                        {
                            var newValue = kvp.Value.Converter.Convert(value);
                            if (newValue != null)
                            {
                                value = newValue;
                            }
                            else
                            {
                                Context.Log(LogSeverity.Debug, this, "failed converting '{OriginalColumn}' in row #{RowIndex}: '{ValueAsString}' ({ValueType}) using {ConverterType}", kvp.Key, ri, value.ToString(), value.GetType().Name, kvp.Value.Converter.GetType().Name);

                                row.SetValue(kvp.Key, new EtlRowError()
                                {
                                    Process = this,
                                    Operation = null,
                                    OriginalValue = value,
                                    Message = string.Format("failed to convert by {0}", kvp.Value.Converter.GetType().Name),
                                }, this);

                                continue;
                            }
                        }
                        else
                        {
                            if (value == null && kvp.Value.ValueIfNull != null)
                            {
                                value = kvp.Value.ValueIfNull;
                            }
                        }

                        row.SetValue(kvp.Key, value, this);
                    }

                    if (IgnoreNullOrEmptyRows && row.IsNullOrEmpty()) continue;

                    resultCount++;
                    index++;
                    if (AddRowIndexToColumn != null) row.SetValue(AddRowIndexToColumn, index, this);
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }
    }
}