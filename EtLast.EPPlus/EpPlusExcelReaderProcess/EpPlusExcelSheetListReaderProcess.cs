namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusExcelSheetListReaderProcess : AbstractBaseProducerProcess
    {
        public string FileName { get; set; }

        public EpPlusExcelSheetListReaderProcess(IEtlContext context, string name) : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(ICaller caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            var startedOn = Stopwatch.StartNew();

            foreach (var row in EvaluateInputProcess(startedOn))
                yield return row;

            var resultCount = 0;
            Context.Log(LogSeverity.Debug, this, "reading from {FileName}", FileName);

            ExcelPackage package;
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
                if (workbook == null || workbook.Worksheets.Count == 0)
                    yield break;

                for (var i = 0; i < workbook.Worksheets.Count; i++)
                {
                    var row = Context.CreateRow(4);
                    row.SetValue("Index", i, this);
                    row.SetValue("Name", workbook.Worksheets[i].Name, this);
                    row.SetValue("Color", workbook.Worksheets[i].TabColor, this);
                    row.SetValue("Visible", workbook.Worksheets[i].Hidden == eWorkSheetHidden.Visible, this);
                    yield return row;
                    resultCount++;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, startedOn.Elapsed);
        }
    }
}