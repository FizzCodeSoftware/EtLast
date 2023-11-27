namespace FizzCode.EtLast;

public sealed class EpPlusExcelSheetListReader: AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required IStreamProvider StreamProvider { get; init; }

    /// <summary>
    /// Default value is "Stream".
    /// </summary>
    public string AddStreamNameToColumn { get; init; } = "Stream";

    public override string GetTopic()
    {
        return StreamProvider.GetTopic() + "[SheetList]";
    }

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> Produce()
    {
        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (FlowState.IsTerminating)
                break;

            ExcelPackage package;
            try
            {
                package = new ExcelPackage(stream.Stream);
            }
            catch (Exception ex)
            {
                var exception = new StreamReadException(this, "excel steram read failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel stream read failed: {0}, message: {1}",
                    stream.Name, ex.Message));
                exception.Data["StreamName"] = stream.Name;

                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, null, exception);
                throw exception;
            }

            var rowCount = 0L;
            package.Compatibility.IsWorksheets1Based = false;
            var workbook = package.Workbook;
            if (workbook == null)
            {
                var exception = new StreamReadException(this, "excel stream read failed");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "excel stream read failed: {0}",
                    stream.Name));
                exception.Data["StreamName"] = stream.Name;

                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);
                throw exception;
            }

            try
            {
                foreach (var worksheet in workbook.Worksheets)
                {
                    if (FlowState.IsTerminating)
                        yield break;

                    var initialValues = new Dictionary<string, object>
                    {
                        ["Index"] = worksheet.Index,
                        ["Name"] = worksheet.Name,
                        ["Color"] = worksheet.TabColor,
                        ["Visible"] = worksheet.Hidden == eWorkSheetHidden.Visible,
                    };

                    if (AddStreamNameToColumn != null)
                    {
                        initialValues["Stream"] = stream.Name;
                    }

                    rowCount++;
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
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusExcelSheetListReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadSheetListFromExcel(this IFluentSequenceBuilder builder, EpPlusExcelSheetListReader reader)
    {
        return builder.ReadFrom(reader);
    }
}
