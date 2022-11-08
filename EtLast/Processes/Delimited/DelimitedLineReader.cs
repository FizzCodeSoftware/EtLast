namespace FizzCode.EtLast;

public enum DelimitedLineHeader { NoHeader, HasHeader, IgnoreHeader }

public sealed class DelimitedLineReader : AbstractRowSource
{
    public IStreamProvider StreamProvider { get; init; }

    public Dictionary<string, ReaderColumn> Columns { get; init; }
    public ReaderDefaultColumn DefaultColumns { get; init; }

    /// <summary>
    /// Default true.
    /// </summary>
    public bool TreatEmptyStringAsNull { get; init; } = true;

    /// <summary>
    /// Default true. If a value starts and ends with double quote (") characters, then both will be removed (this happens before type conversion)
    /// </summary>
    public bool RemoveSurroundingDoubleQuotes { get; init; } = true;

    /// <summary>
    /// Default <see cref="DelimitedLineHeader.NoHeader"/>.
    /// </summary>
    public DelimitedLineHeader Header { get; init; }

    /// <summary>
    /// Default null. Column names must be set if <see cref="Header"/> is <see cref="DelimitedLineHeader.NoHeader"/> or <see cref="DelimitedLineHeader.IgnoreHeader"/>, otherwise it should be left null.
    /// </summary>
    public string[] ColumnNames { get; init; }

    /// <summary>
    /// Default null.
    /// </summary>
    public string[] IgnoreColumns { get; init; }

    /// <summary>
    /// Default value is ';'.
    /// </summary>
    public char Delimiter { get; init; } = ';';

    /// <summary>
    /// Default value is 0
    /// </summary>
    public int SkipLinesAtBeginning { get; init; }

    /// <summary>
    /// Default value is \r\n
    /// </summary>
    public string LineEnding { get; init; } = "\r\n";

    public DelimitedLineReader(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return StreamProvider?.GetTopic();
    }

    protected override void ValidateImpl()
    {
        if (StreamProvider == null)
            throw new ProcessParameterNullException(this, nameof(StreamProvider));

        StreamProvider.Validate(this);

        if (Header != DelimitedLineHeader.HasHeader && (ColumnNames == null || ColumnNames.Length == 0))
            throw new ProcessParameterNullException(this, nameof(ColumnNames));

        if (Header == DelimitedLineHeader.HasHeader && ColumnNames?.Length > 0)
            throw new InvalidProcessParameterException(this, nameof(ColumnNames), ColumnNames, nameof(ColumnNames) + " must be null if " + nameof(Header) + " is true.");

        if (Columns == null && DefaultColumns == null)
            throw new InvalidProcessParameterException(this, nameof(Columns), Columns, nameof(DefaultColumns) + " must be specified if " + nameof(Columns) + " is null.");
    }

    protected override IEnumerable<IRow> Produce()
    {
        // key is the SOURCE col name
        var columnMap = Columns?.ToDictionary(
            kvp => kvp.Value.SourceColumn ?? kvp.Key,
            kvp => (rowColumn: kvp.Key, config: kvp.Value),
            StringComparer.InvariantCultureIgnoreCase);

        var resultCount = 0;

        var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        var partList = new List<string>(100);
        var builder = new StringBuilder(2000);

        // capture for performance
        var delimiter = Delimiter;
        var treatEmptyStringAsNull = TreatEmptyStringAsNull;
        var removeSurroundingDoubleQuotes = RemoveSurroundingDoubleQuotes;
        var ignoreColumns = IgnoreColumns?.ToHashSet();
        var skipLines = SkipLinesAtBeginning;

        var streams = StreamProvider.GetStreams(this);
        if (streams == null)
            yield break;

        foreach (var stream in streams)
        {
            if (stream == null)
                yield break;

            if (Pipe.IsTerminating)
                break;

            var firstRow = true;
            var columnNames = ColumnNames;

            //Console.WriteLine("new stream :" + stream.Stream.Length);

            StreamReader reader = null;
            try
            {
                const int bufferSize = 8192;
                var buffer = new char[bufferSize];
                var bufferPosition = 0;
                var bufferLength = 0;

                reader = new StreamReader(stream.Stream, bufferSize: 1024);

                //var read = reader.ReadBlock(buffer, 0, bufferSize);
                var read = reader.ReadBlock(buffer.AsSpan());
                bufferLength = read;
                bufferPosition = 0;

                var quotes = 0;
                var builderLength = 0;
                var cellStartsWithQuote = false;

                var fileCompleted = false;
                var noMoreData = false;
                var lineCompleted = false;
                while (!fileCompleted)
                {
                    var remaining = bufferLength - bufferPosition;
                    if (remaining < 4 && !noMoreData)
                    {
                        if (remaining > 0)
                            Array.Copy(buffer, bufferPosition, buffer, 0, remaining);

                        //read = reader.ReadBlock(buffer, remaining, bufferSize - remaining);
                        read = reader.ReadBlock(buffer.AsSpan(remaining, bufferSize - remaining));
                        if (read == 0)
                            noMoreData = true;

                        bufferLength = remaining + read;
                        bufferPosition = 0;
                    }

                    lineCompleted = false;

                    if (bufferPosition < bufferLength)
                    {
                        var c = buffer[bufferPosition++];

                        var nc = bufferPosition < bufferLength
                            ? buffer[bufferPosition]
                            : '\0';

                        if (c is '\r' or '\n')
                        {
                            if (nc is '\r' or '\n')
                                bufferPosition++;

                            lineCompleted = true;
                        }
                        else
                        {
                            var isQuote = c == '\"';
                            var lastCharInLine = nc is '\r' or '\n';
                            //var nextCharIsQuote = /*!lastCharInLine && */nc == '\"';

                            if (builderLength == 0 && isQuote)
                                quotes++;

                            // quotedCellClosing
                            if (builderLength > 0 && isQuote && quotes > 0 && nc == delimiter)
                                quotes--;

                            // newLineInQuotedCell
                            if (builderLength > 0 && cellStartsWithQuote)
                            {
                                if (!isQuote && lastCharInLine)
                                {
                                    // add char
                                    builder.Append(c);

                                    // add newline
                                    builder.Append(nc);

                                    builderLength += 2;

                                    // skip newline
                                    bufferPosition++;

                                    // peek for a possible \n after \r
                                    if (bufferPosition < bufferLength)
                                    {
                                        var secondNewLineC = buffer[bufferPosition];
                                        if (secondNewLineC is '\r' or '\n')
                                        {
                                            builder.Append(secondNewLineC);
                                            builderLength++;

                                            bufferPosition++;
                                        }
                                    }
                                    //lastCharInLine = false;
                                    continue;
                                }
                                else if (isQuote && nc == '\"'/*nextCharIsQuote*/ && bufferPosition <= bufferLength - 3)
                                {
                                    var nnc = buffer[bufferPosition + 1];
                                    if (nnc is '\r' or '\n')
                                    {
                                        builder.Append(c);

                                        if (quotes > 0 && builderLength > 0)
                                        {
                                            // Skip next quote. RFC 4180, 2/7: If double-quotes are used to enclose fields, then a double-quote appearing inside a field must be escaped by preceding it with another double quote.
                                        }
                                        else
                                        {
                                            builder.Append(nc);
                                            builderLength++;
                                        }

                                        builder.Append(nnc);

                                        builderLength += 2;

                                        // skip quote
                                        bufferPosition++;

                                        // skip newline
                                        bufferPosition++;

                                        // peek for a possible \n after \r
                                        if (bufferPosition < bufferLength)
                                        {
                                            var secondNewLineC = buffer[bufferPosition];
                                            if (secondNewLineC is '\r' or '\n')
                                            {
                                                builder.Append(secondNewLineC);
                                                builderLength++;
                                                bufferPosition++;
                                            }
                                        }

                                        //lastCharInLine = false;
                                        continue;
                                    }
                                }
                            }

                            if (quotes > 0 || c != delimiter)
                            {
                                builder.Append(c);
                                if (builderLength == 0 && isQuote)
                                    cellStartsWithQuote = true;

                                if (quotes > 0 && isQuote && nc == '\"'/*nextCharIsQuote*/ && builderLength > 0)
                                {
                                    bufferPosition++;
                                    // Skip next quote. RFC 4180, 2/7: If double-quotes are used to enclose fields, then a double-quote appearing inside a field must be escaped by preceding it with another double quote.
                                }

                                builderLength++;
                            }

                            if (lastCharInLine || (quotes == 0 && c == delimiter))
                            {
                                if (builderLength == 0)
                                {
                                    partList.Add(string.Empty);
                                }
                                else
                                {
                                    // x
                                    if (removeSurroundingDoubleQuotes
                                        && builderLength > 1
                                        && cellStartsWithQuote
                                        && builder[builderLength - 1] == '\"')
                                    {
                                        if (builderLength > 2)
                                        {
                                            partList.Add(builder.ToString(1, builderLength - 2));
                                        }
                                        else
                                        {
                                            partList.Add(string.Empty);
                                        }
                                    }
                                    else
                                    {
                                        partList.Add(builder.ToString());
                                    }
                                    // x

                                    builder.Clear();
                                    builderLength = 0;
                                    cellStartsWithQuote = false;
                                }
                            }

                        }
                    }
                    else
                    {
                        fileCompleted = true;

                        if (builderLength > 0)
                        {
                            // x
                            if (removeSurroundingDoubleQuotes
                                && builderLength > 1
                                && cellStartsWithQuote
                                && builder[builderLength - 1] == '\"')
                            {
                                if (builderLength > 2)
                                {
                                    partList.Add(builder.ToString(1, builderLength - 2));
                                }
                                else
                                {
                                    partList.Add(string.Empty);
                                }
                            }
                            else
                            {
                                partList.Add(builder.ToString());
                            }
                            // x

                            builder.Clear();
                            builderLength = 0;
                            cellStartsWithQuote = false;
                        }
                    }

                    if (lineCompleted || fileCompleted)
                    {
                        if (skipLines > 0)
                        {
                            skipLines--;
                            partList.Clear();
                            builder.Clear();
                            builderLength = 0;
                            quotes = 0;
                            cellStartsWithQuote = false;
                            continue;
                        }

                        if (firstRow)
                        {
                            firstRow = false;

                            if (Header != DelimitedLineHeader.NoHeader)
                            {
                                if (Header == DelimitedLineHeader.HasHeader)
                                {
                                    columnNames = partList.ToArray();

                                    for (var i = 0; i < columnNames.Length - 1; i++)
                                    {
                                        var columnName = columnNames[i];
                                        for (var j = i + 1; j < columnNames.Length; j++)
                                        {
                                            if (string.Equals(columnName, columnNames[j], StringComparison.InvariantCultureIgnoreCase))
                                            {
                                                var exception = new DelimitedReadException(this, "delimited input contains more than one columns with the same name", stream);
                                                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "delimited input contains more than one columns with the same name: {0}, {1}", stream.Name, columnName));
                                                exception.Data["Column"] = columnName;

                                                Context.RegisterIoCommandFailed(this, stream.IoCommandKind, stream.IoCommandUid, 0, exception);
                                                throw exception;
                                            }
                                        }
                                    }
                                }

                                partList.Clear();
                                builder.Clear();
                                builderLength = 0;
                                quotes = 0;
                                cellStartsWithQuote = false;
                                continue;
                            }
                        }

                        initialValues.Clear();
                        var colCnt = Math.Min(columnNames.Length, partList.Count);

                        for (var i = 0; i < colCnt; i++)
                        {
                            var csvColumn = columnNames[i];
                            if (ignoreColumns?.Contains(csvColumn) == true)
                                continue;

                            var str = partList[i];
                            var value = treatEmptyStringAsNull && string.IsNullOrEmpty(str)
                                ? null
                                : str;

                            if (columnMap != null && columnMap.TryGetValue(csvColumn, out var col))
                            {
                                try
                                {
                                    initialValues[col.rowColumn] = col.config.Process(this, value);
                                }
                                catch (Exception ex)
                                {
                                    initialValues[col.rowColumn] = new EtlRowError(this, value, ex);
                                }
                            }
                            else if (DefaultColumns != null)
                            {
                                try
                                {
                                    initialValues[csvColumn] = DefaultColumns.Process(this, value);
                                }
                                catch (Exception ex)
                                {
                                    initialValues[csvColumn] = new EtlRowError(this, value, ex);
                                }
                            }
                        }

                        partList.Clear();
                        builder.Clear();
                        builderLength = 0;
                        quotes = 0;
                        cellStartsWithQuote = false;

                        resultCount++;
                        yield return Context.CreateRow(this, initialValues);

                        if (Pipe.IsTerminating)
                            break;
                    }
                }
            }
            finally
            {
                if (stream != null)
                {
                    Context.RegisterIoCommandSuccess(this, stream.IoCommandKind, stream.IoCommandUid, resultCount);
                    stream.Dispose();
                    reader?.Dispose();
                }
            }
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DelimitedFileReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadDelimitedLines(this IFluentSequenceBuilder builder, DelimitedLineReader reader)
    {
        return builder.ReadFrom(reader);
    }
}
