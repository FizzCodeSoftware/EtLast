namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusExcelSheetListReaderProcess : AbstractProducerProcess
    {
        public string FileName { get; set; }

        public EpPlusExcelSheetListReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
        }

        protected override IEnumerable<IRow> Produce()
        {
            Context.Log(LogSeverity.Debug, this, "reading from {FileName}", FileName);

            ExcelPackage package;
            try
            {
                package = new ExcelPackage(new FileInfo(FileName));
            }
            catch (Exception ex)
            {
                var exception = new EtlException(this, "excel file read failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}", FileName, ex.Message));
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
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    var row = Context.CreateRow(4);
                    row.SetValue("Index", i, this);
                    row.SetValue("Name", workbook.Worksheets[i].Name, this);
                    row.SetValue("Color", workbook.Worksheets[i].TabColor, this);
                    row.SetValue("Visible", workbook.Worksheets[i].Hidden == eWorkSheetHidden.Visible, this);

                    yield return row;
                }
            }
        }
    }
}