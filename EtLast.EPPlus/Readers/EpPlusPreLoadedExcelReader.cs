namespace FizzCode.EtLast;

public sealed class EpPlusPreLoadedExcelReader : AbstractEpPlusExcelReader
{
    /// <summary>
    /// Usage: reader.PreLoadedFile = new ExcelPackage(new FileInfo(fileName));
    /// </summary>
    public required ExcelPackage PreLoadedFile { get; init; }

    public override string GetTopic()
    {
        if (PreLoadedFile.File?.Name != null)
            return Path.GetFileName(PreLoadedFile.File.Name);

        return null;
    }

    protected override void ValidateImpl()
    {
        if (PreLoadedFile == null)
            throw new ProcessParameterNullException(this, nameof(PreLoadedFile));

        if (string.IsNullOrEmpty(SheetName) && SheetIndex == -1)
            throw new ProcessParameterNullException(this, nameof(SheetName));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    protected override IEnumerable<IRow> Produce()
    {
        return ProduceFrom(null, PreLoadedFile, 0, null);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class EpPlusPreLoadedExcelReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromPreLoadedExcel(this IFluentSequenceBuilder builder, EpPlusPreLoadedExcelReader reader)
    {
        return builder.ReadFrom(reader);
    }
}