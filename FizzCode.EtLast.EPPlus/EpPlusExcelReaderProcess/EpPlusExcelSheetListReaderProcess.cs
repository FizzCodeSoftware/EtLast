namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    public class EpPlusExcelSheetListReaderProcess : IProcess
    {
        public IEtlContext Context { get; }
        public string Name { get; }

        public IProcess Caller { get; private set; }
        public IProcess InputProcess { get; set; }
        public string FileName { get; set; }

        public EpPlusExcelSheetListReaderProcess(IEtlContext context, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName)) throw new ProcessParameterNullException(this, nameof(FileName));

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
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", rowCount, InputProcess.Name, sw.Elapsed);
            }

            var resultCount = 0;
            Context.Log(LogSeverity.Information, this, "reading from {FileName}", FileName);

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

                for (int i = 0; i < workbook.Worksheets.Count; i++)
                {
                    var row = Context.CreateRow(4);
                    row.SetValue("Index", i, this);
                    row.SetValue("Name", workbook.Worksheets[i].Name, this);
                    row.SetValue("Color", workbook.Worksheets[i].TabColor, this);
                    row.SetValue("Visible", workbook.Worksheets[i].Hidden == eWorkSheetHidden.Visible, this);
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }
    }
}