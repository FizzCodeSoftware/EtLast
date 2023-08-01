namespace FizzCode.EtLast;

public sealed class WriteToDynamicDelimitedMutator : AbstractMutator, IRowSink
{
    public required ISinkProvider SinkProvider { get; init; }

    /// <summary>
    /// Default value is <see cref="Encoding.UTF8"/>
    /// </summary>
    public required Encoding Encoding { get; init; } = Encoding.UTF8;

    public IValueFormatter ValueFormatter { get; init; } = DelimitedValueFormatter.Default;

    /// <summary>
    /// Default value is <see cref="CultureInfo.InvariantCulture"/>;
    /// </summary>
    public required IFormatProvider FormatProvider { get; init; } = CultureInfo.InvariantCulture;

    /// <summary>
    /// Default value is \r\n
    /// </summary>
    public string LineEnding { get; init; } = "\r\n";

    /// <summary>
    /// Default value is "
    /// </summary>
    public char Quote { get; init; } = '\"';

    /// <summary>
    /// Default value is "
    /// </summary>
    public char Escape { get; init; } = '\"';

    /// <summary>
    /// Default value is ';'.
    /// </summary>
    public required char Delimiter { get; init; } = ';';

    /// <summary>
    /// Default value is true
    /// </summary>
    public required bool WriteHeader { get; init; } = true;

    public PartitionKeyGenerator PartitionKeyGenerator { get; init; }

    private readonly Dictionary<string, NamedSinkWithColumns> _sinksWithColumns = new();
    private byte[] _delimiterBytes;
    private byte[] _lineEndingBytes;
    private string _escapedQuote;
    private char[] _quoteRequiredChars;
    private string _quoteAsString;

    private int _rowCounter;

    public WriteToDynamicDelimitedMutator(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (SinkProvider == null)
            throw new ProcessParameterNullException(this, nameof(SinkProvider));

        SinkProvider.Validate(this);
    }

    protected override void StartMutator()
    {
        _delimiterBytes = Encoding.GetBytes(new[] { Delimiter });
        _lineEndingBytes = Encoding.GetBytes(LineEnding);
        _escapedQuote = new string(new[] { Escape, Quote });
        _quoteRequiredChars = new[] { Delimiter, Quote, Escape, '\r', '\n' };
        _quoteAsString = Quote.ToString();

        _rowCounter = 0;
    }

    private NamedSinkWithColumns GetSinkWithColumns(string partitionKey, IReadOnlySlimRow firstRow)
    {
        var internalKey = partitionKey ?? "\0__nopartition__\0";

        if (_sinksWithColumns.TryGetValue(internalKey, out var sinkWithColumns))
            return sinkWithColumns;

        sinkWithColumns = new NamedSinkWithColumns()
        {
            Sink = SinkProvider.GetSink(this, partitionKey),
            Columns = firstRow.Values.Select(x => x.Key).ToArray(),
        };

        _sinksWithColumns.Add(internalKey, sinkWithColumns);

        if (WriteHeader)
        {
            if (sinkWithColumns.Sink.SafeGetPosition() == 0)
            {
                var first = true;
                foreach (var columnName in sinkWithColumns.Columns)
                {
                    if (!first)
                        sinkWithColumns.Sink.Stream.Write(_delimiterBytes);

                    var quoteRequired = !string.IsNullOrEmpty(columnName) &&
                        (columnName.IndexOfAny(_quoteRequiredChars) > -1
                        || columnName[0] == ' '
                        || columnName[^1] == ' '
                        || columnName.Contains(LineEnding, StringComparison.Ordinal));

                    var line = ConvertToDelimitedValue(columnName, quoteRequired);
                    sinkWithColumns.Sink.Stream.Write(Encoding.GetBytes(line));

                    first = false;
                }

                sinkWithColumns.Sink.IncreaseRowsWritten();
            }
            else
            {
                sinkWithColumns.Sink.Stream.Write(_lineEndingBytes);
            }
        }

        return sinkWithColumns;
    }

    protected override void CloseMutator()
    {
        if (SinkProvider.AutomaticallyDispose)
        {
            foreach (var sinkWithColumns in _sinksWithColumns.Values)
            {
                sinkWithColumns.Sink.Stream.Flush();
                sinkWithColumns.Sink.Stream.Close();
                sinkWithColumns.Sink.Stream.Dispose();
            }
        }

        _sinksWithColumns.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var partitionKey = PartitionKeyGenerator?.Invoke(row, _rowCounter);
        _rowCounter++;

        var sinkWithColumns = GetSinkWithColumns(partitionKey, row);

        Context.RegisterWriteToSink(row, sinkWithColumns.Sink.SinkUid);

        try
        {
            if (sinkWithColumns.Sink.RowsWritten > 0)
                sinkWithColumns.Sink.Stream.Write(_lineEndingBytes);

            var first = true;
            foreach (var columnName in sinkWithColumns.Columns)
            {
                if (!first)
                    sinkWithColumns.Sink.Stream.Write(_delimiterBytes);

                var value = row[columnName];

                var str = ValueFormatter.Format(value, FormatProvider);
                var quoteRequired = !string.IsNullOrEmpty(str) &&
                    (str.IndexOfAny(_quoteRequiredChars) > -1
                    || str[0] == ' '
                    || str[^1] == ' '
                    || str.Contains(LineEnding, StringComparison.Ordinal));

                var convertedValue = ConvertToDelimitedValue(str, quoteRequired);
                if (convertedValue != null)
                {
                    sinkWithColumns.Sink.Stream.Write(Encoding.GetBytes(convertedValue));
                }

                first = false;
            }

            sinkWithColumns.Sink.IncreaseRowsWritten();
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, sinkWithColumns.Sink.IoCommandKind, sinkWithColumns.Sink.IoCommandUid, sinkWithColumns.Sink.RowsWritten, ex);
            throw;
        }

        yield return row;
    }

    private string ConvertToDelimitedValue(string value, bool quoteRequired)
    {
        if (quoteRequired)
        {
            if (value != null)
            {
                value = value.Replace(_quoteAsString, _escapedQuote, StringComparison.Ordinal);
            }

            value = Quote + value + Quote;
        }

        return value;
    }

    private class NamedSinkWithColumns
    {
        public required NamedSink Sink { get; init; }
        public required string[] Columns { get; init; }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteToUnstructuredDelimitedMutatorFluent
{
    /// <summary>
    /// Write rows to a delimited stream. The first row if each partition is used to determine the columns of the delimited output.
    /// Because rows doesn't store null values by nature, it is recommended to be sure all values are non-nullable,
    /// or use rows with <see cref="IReadOnlySlimRow.KeepNulls"/> enabled in a sequence which uses this mutator.
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToDynamicDelimited(this IFluentSequenceMutatorBuilder builder, WriteToDynamicDelimitedMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}