namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using OfficeOpenXml;

    public sealed class EpPlusExcelReader : AbstractEpPlusExcelReader
    {
        public IStreamProvider StreamProvider { get; init; }

        public EpPlusExcelReader(IEtlContext context)
            : base(context)
        {
        }

        public override string GetTopic()
        {
            if (string.IsNullOrEmpty(SheetName))
                return StreamProvider.Topic + "[" + SheetIndex.ToString("D", CultureInfo.InvariantCulture) + "]";
            else
                return StreamProvider.Topic + "[" + SheetName + "]";
        }

        protected override void ValidateImpl()
        {
            if (StreamProvider == null)
                throw new ProcessParameterNullException(this, nameof(StreamProvider));

            if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
                throw new ProcessParameterNullException(this, nameof(SheetName));

            if (Columns == null)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }

        protected override IEnumerable<IRow> Produce()
        {
            foreach (var stream in StreamProvider.GetStreams(this))
            {
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
                try
                {
                    foreach (var row in ProduceFrom(stream, package))
                    {
                        rowCount++;
                        yield return row;
                    }
                }
                finally
                {
                    Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandUid, rowCount);
                    stream.Dispose();
                    package.Dispose();
                }
            }
        }

        private static string EnsureDistinctColumnNames(List<string> excelColumns, string excelColumn)
        {
            var col = excelColumn;
            var i = 1;
            while (excelColumns.Contains(col))
            {
                col = excelColumn + i.ToString("D", CultureInfo.InvariantCulture);
                i++;
            }

            excelColumns.Add(col);
            return col;
        }

        private ExcelRange GetCellUnmerged(ExcelWorksheet sheet, int row, int col)
        {
            if (!Unmerge)
                return sheet.Cells[row, col];

            var mergedCellAddress = sheet.MergedCells[row, col];
            if (mergedCellAddress == null)
                return sheet.Cells[row, col];

            var address = new ExcelAddress(mergedCellAddress);
            return sheet.Cells[address.Start.Address];
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusExcelReaderFluent
    {
        public static IFluentProcessMutatorBuilder ReadFromExcel(this IFluentProcessBuilder builder, EpPlusExcelReader reader)
        {
            return builder.ReadFrom(reader);
        }
    }
}