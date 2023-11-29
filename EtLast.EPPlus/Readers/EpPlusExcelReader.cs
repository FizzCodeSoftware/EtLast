﻿namespace FizzCode.EtLast;

public sealed class EpPlusExcelReader : AbstractEpPlusExcelReader
{
    [ProcessParameterMustHaveValue]
    public required IStreamProvider StreamProvider { get; init; }

    /// <summary>
    /// First stream index is (integer) 0
    /// </summary>
    public string AddStreamIndexToColumn { get; init; }

    public override string GetTopic()
    {
        if (string.IsNullOrEmpty(SheetName))
            return StreamProvider.GetTopic() + "[" + SheetIndex.ToString("D", CultureInfo.InvariantCulture) + "]";
        else
            return StreamProvider.GetTopic() + "[" + SheetName + "]";
    }

    protected override void ValidateImpl()
    {
        if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
            throw new ProcessParameterNullException(this, nameof(SheetName));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    protected override IEnumerable<IRow> Produce()
    {
        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        var rowCount = 0L;
        var streamIndex = 0;
        foreach (var stream in streams)
        {
            if (stream == null)
                continue;

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

                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandId, null, exception);
                throw exception;
            }

            try
            {
                foreach (var row in ProduceFrom(stream, package, streamIndex, AddStreamIndexToColumn))
                {
                    rowCount++;
                    yield return row;
                }
            }
            finally
            {
                Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandId, rowCount);
                stream.Dispose();
                package.Dispose();
            }

            streamIndex++;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusExcelReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromExcel(this IFluentSequenceBuilder builder, EpPlusExcelReader reader)
    {
        return builder.ReadFrom(reader);
    }
}
