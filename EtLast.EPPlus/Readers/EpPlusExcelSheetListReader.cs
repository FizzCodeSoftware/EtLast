namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public sealed class EpPlusExcelSheetListReader : AbstractRowSource, IRowSource
    {
        public string FileName { get; init; }

        public EpPlusExcelSheetListReader(IEtlContext context, string topic, string name)
            : base(context, topic, name)
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
                var exception = new FileReadException(this, "input file doesn't exist", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exist: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                throw exception;
            }

            ExcelPackage package;
            try
            {
                package = new ExcelPackage(new FileInfo(FileName));
            }
            catch (Exception ex)
            {
                var exception = new FileReadException(this, "excel file read failed", FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, null, ex);
                throw exception;
            }

            var rowCount = 0;
            package.Compatibility.IsWorksheets1Based = false;
            var workbook = package.Workbook;
            if (workbook == null)
            {
                var exception = new FileReadException(this, "excel file read failed", FileName);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel file read failed, file name: {0}",
                    FileName));
                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
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

            Context.RegisterIoCommandSuccess(this, IoCommandKind.fileRead, iocUid, rowCount);
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusExcelSheetListReaderFluent
    {
        public static IFluentProcessMutatorBuilder ReadSheetListFromExcel(this IFluentProcessBuilder builder, EpPlusExcelSheetListReader reader)
        {
            return builder.ReadFrom(reader);
        }
    }
}