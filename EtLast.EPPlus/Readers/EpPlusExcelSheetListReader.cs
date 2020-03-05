namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public class EpPlusExcelSheetListReader : AbstractProducer, IRowReader
    {
        public string FileName { get; set; }

        public EpPlusExcelSheetListReader(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
        }

        protected override IEnumerable<IRow> Produce()
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.fileRead, PathHelpers.GetFriendlyPathName(FileName), null, null, null, null,
                "reading from {FileName}",
                PathHelpers.GetFriendlyPathName(FileName));

            if (!File.Exists(FileName))
            {
                var exception = new ProcessExecutionException(this, "input file doesn't exist");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exist: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, iocUid, 0, exception);
                throw exception;
            }

            ExcelPackage package;
            try
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                package = new ExcelPackage(new FileInfo(FileName));
#pragma warning restore CA2000 // Dispose objects before losing scope
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "excel file read failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, iocUid, 0, ex);
                throw exception;
            }

            var rowCount = 0;
            package.Compatibility.IsWorksheets1Based = false;
            var workbook = package.Workbook;
            if (workbook == null)
            {
                var exception = new ProcessExecutionException(this, "excel file read failed");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, iocUid, 0, exception);
                throw exception;
            }

            try
            {
                foreach (var worksheet in workbook.Worksheets)
                {
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    var initialValues = new Dictionary<string, object>
                    {
                        ["Index"] = worksheet.Index,
                        ["Name"] = worksheet.Name,
                        ["Color"] = worksheet.TabColor,
                        ["Visible"] = worksheet.Hidden == eWorkSheetHidden.Visible,
                    };

                    yield return Context.CreateRow(this, initialValues);
                }
            }
            finally
            {
                package.Dispose();
            }

            Context.RegisterIoCommandSuccess(this, iocUid, rowCount);
        }
    }
}