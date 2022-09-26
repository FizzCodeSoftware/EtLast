namespace FizzCode.EtLast;

public sealed class WriteToDelimitedMutator : AbstractMutator, IRowSink
{
    public ISinkProvider SinkProvider { get; init; }

    /// <summary>
    /// Default value is <see cref="Encoding.UTF8"/>
    /// </summary>
    public Encoding Encoding { get; init; } = Encoding.UTF8;

    /// <summary>
    /// Default value is <see cref="CultureInfo.InvariantCulture"/>;
    /// </summary>
    public IFormatProvider FormatProvider { get; init; } = CultureInfo.InvariantCulture;

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
    public char Delimiter { get; init; } = ';';

    /// <summary>
    /// Default value is true
    /// </summary>
    public bool WriteHeader { get; init; } = true;

    /// <summary>
    /// Key is the output column title AND the column in the row (later can be customized by setting a <see cref="DelimitedColumn"/>).
    /// </summary>
    public Dictionary<string, DelimitedColumn> Columns { get; init; }

    public PartitionKeyGenerator PartitionKeyGenerator { get; set; }

    private readonly Dictionary<string, NamedSink> _sinks = new();
    private byte[] _delimiterBytes;
    private byte[] _lineEndingBytes;
    private string _escapedQuote;
    private char[] _quoteRequiredChars;
    private string _quoteAsString;

    private int _rowCounter;

    public WriteToDelimitedMutator(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (SinkProvider == null)
            throw new ProcessParameterNullException(this, nameof(SinkProvider));

        SinkProvider.Validate(this);

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
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

    private NamedSink GetSink(string partitionKey)
    {
        var internalKey = partitionKey ?? "\0__nopartition__\0";

        if (_sinks.TryGetValue(internalKey, out var sink))
            return sink;

        sink = SinkProvider.GetSink(this, partitionKey);
        _sinks.Add(internalKey, sink);

        if (WriteHeader)
        {
            var first = true;
            foreach (var kvp in Columns)
            {
                if (!first)
                    sink.Stream.Write(_delimiterBytes);

                var str = kvp.Key;
                var quoteRequired = !string.IsNullOrEmpty(str) &&
                    (str.IndexOfAny(_quoteRequiredChars) > -1
                    || str[0] == ' '
                    || str[^1] == ' '
                    || str.Contains(LineEnding, StringComparison.Ordinal));

                var line = ConvertToDelimitedValue(str, quoteRequired);
                sink.Stream.Write(Encoding.GetBytes(line));

                first = false;
            }

            sink.IncreaseRowsWritten();
        }

        return sink;
    }

    protected override void CloseMutator()
    {
        if (SinkProvider.AutomaticallyDispose)
        {
            foreach (var sink in _sinks.Values)
            {
                sink.Stream.Flush();
                sink.Stream.Close();
                sink.Stream.Dispose();
            }
        }

        _sinks.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var partitionKey = PartitionKeyGenerator?.Invoke(row, _rowCounter);
        _rowCounter++;

        var sink = GetSink(partitionKey);

        Context.RegisterWriteToSink(row, sink.SinkUid);

        try
        {
            if (sink.RowsWritten > 0)
                sink.Stream.Write(_lineEndingBytes);

            var first = true;
            foreach (var kvp in Columns)
            {
                if (!first)
                    sink.Stream.Write(_delimiterBytes);

                var value = row[kvp.Value?.SourceColumn ?? kvp.Key];

                var str = (kvp.Value?.CustomFormatter ?? ValueFormatter.Default).Format(value, FormatProvider);
                var quoteRequired = !string.IsNullOrEmpty(str) &&
                    (str.IndexOfAny(_quoteRequiredChars) > -1
                    || str[0] == ' '
                    || str[^1] == ' '
                    || str.Contains(LineEnding, StringComparison.Ordinal));

                var convertedValue = ConvertToDelimitedValue(str, quoteRequired);
                if (convertedValue != null)
                {
                    sink.Stream.Write(Encoding.GetBytes(convertedValue));
                }

                first = false;
            }

            sink.IncreaseRowsWritten();
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, sink.IoCommandKind, sink.IoCommandUid, sink.RowsWritten, ex);
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
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class WriteToDelimitedMutatorFluent
{
    /// <summary>
    /// Write rows to a delimited stream.
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToDelimited(this IFluentSequenceMutatorBuilder builder, WriteToDelimitedMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
