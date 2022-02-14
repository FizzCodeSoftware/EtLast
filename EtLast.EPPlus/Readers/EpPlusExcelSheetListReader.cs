namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using OfficeOpenXml;

    public sealed class EpPlusExcelSheetListReader : AbstractRowSource, IRowSource
    {
        public IStreamSource Source { get; init; }

        public EpPlusExcelSheetListReader(IEtlContext context)
            : base(context)
        {
        }

        public override string GetTopic()
        {
            return Source.Topic + "[SheetList]";
        }

        protected override void ValidateImpl()
        {
            if (Source == null)
                throw new ProcessParameterNullException(this, nameof(Source));
        }

        protected override IEnumerable<IRow> Produce()
        {
            var stream = Source.GetStream(this);
            if (stream == null)
                yield break;

            ExcelPackage package;
            try
            {
                package = new ExcelPackage(stream.Stream);
            }
            catch (Exception ex)
            {
                var exception = new StreamReadException(this, "excel steram read failed", stream, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel stream read failed: {0}, message: {1}",
                    stream.Name, ex.Message));
                exception.Data.Add("StreamName", stream.Name);

                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, null, ex);
                throw exception;
            }

            var rowCount = 0;
            package.Compatibility.IsWorksheets1Based = false;
            var workbook = package.Workbook;
            if (workbook == null)
            {
                var exception = new StreamReadException(this, "excel stream read failed", stream);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel stream read failed: {0}",
                    stream.Name));
                exception.Data.Add("StreamName", stream.Name);

                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);
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

            Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandUid, rowCount);
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