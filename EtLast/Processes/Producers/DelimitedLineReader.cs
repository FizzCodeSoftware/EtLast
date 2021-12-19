namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public sealed class DelimitedLineReader : AbstractRowSource, IRowSource
    {
        public ILineSource LineSource { get; init; }

        public Dictionary<string, ReaderColumnConfiguration> Columns { get; init; }
        public ReaderDefaultColumnConfiguration DefaultColumns { get; init; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; init; } = true;

        /// <summary>
        /// Default true. If a value starts and ends with double quote (") characters, then both will be removed (this happens before type conversion)
        /// </summary>
        public bool RemoveSurroundingDoubleQuotes { get; init; } = true;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool HasHeaderRow { get; init; }

        /// <summary>
        /// Default null. Column names must be set if <see cref="HasHeaderRow"/> is false, otherwise it should be left null.
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

        public DelimitedLineReader(IEtlContext context)
            : base(context)
        {
        }

        public override string GetTopic()
        {
            return LineSource?.GetTopic();
        }

        protected override void ValidateImpl()
        {
            if (LineSource == null)
                throw new ProcessParameterNullException(this, nameof(LineSource));

            if (!HasHeaderRow && (ColumnNames == null || ColumnNames.Length == 0))
                throw new ProcessParameterNullException(this, nameof(ColumnNames));

            if (HasHeaderRow && ColumnNames?.Length > 0)
                throw new InvalidProcessParameterException(this, nameof(ColumnNames), ColumnNames, nameof(ColumnNames) + " must be null if " + nameof(HasHeaderRow) + " is true.");

            if (Columns == null && DefaultColumns == null)
                throw new InvalidProcessParameterException(this, nameof(Columns), Columns, nameof(DefaultColumns) + " must be specified if " + nameof(Columns) + " is null.");
        }

        protected override IEnumerable<IRow> Produce()
        {
            // key is the SOURCE column name
            var columnMap = Columns?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

            var resultCount = 0;

            var firstRow = true;
            var initialValues = new List<KeyValuePair<string, object>>();

            var partList = new List<string>(100);
            var builder = new StringBuilder(2000);

            // capture for performance
            var columnNames = ColumnNames;
            var delimiter = Delimiter;
            var treatEmptyStringAsNull = TreatEmptyStringAsNull;
            var removeSurroundingDoubleQuotes = RemoveSurroundingDoubleQuotes;
            var ignoreColumns = IgnoreColumns?.ToHashSet();

            try
            {
                LineSource.Prepare(this);

                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    var line = LineSource.ReadLine(this);
                    if (line == null)
                        break;

                    if (line.EndsWith(delimiter))
                        line = line[0..^1];

                    if (string.IsNullOrEmpty(line))
                        continue;

                    partList.Clear();
                    builder.Clear();

                    var quotes = 0;
                    var builderLength = 0;
                    var cellStartsWithQuote = false;
                    var lineLength = line.Length;

                    for (var linePos = 0; linePos < lineLength; linePos++)
                    {
                        var c = line[linePos];
                        var isQuote = c == '\"';
                        var lastCharInLine = linePos == lineLength - 1;

                        var nextCharIsDelimiter = false;
                        var nextCharIsQuote = false;
                        if (!lastCharInLine)
                        {
                            var nc = line[linePos + 1];
                            if (nc == delimiter)
                                nextCharIsDelimiter = true;
                            else if (nc == '\"')
                                nextCharIsQuote = true;
                        }

                        if (builderLength == 0 && isQuote)
                            quotes++;

                        var quotedCellClosing = builderLength > 0
                                && isQuote
                                && quotes > 0
                                && nextCharIsDelimiter;

                        if (quotedCellClosing)
                            quotes--;

                        var endOfCell = lastCharInLine || (nextCharIsDelimiter && quotes == 0);

                        var newLineInQuotedCell = builderLength > 0
                            && cellStartsWithQuote
                            && (
                                (!isQuote && lastCharInLine)
                                || (isQuote && nextCharIsQuote && linePos == lineLength - 2)
                                );

                        if (newLineInQuotedCell)
                        {
                            var nextLine = LineSource.ReadLine(this);
                            if (nextLine == null)
                                break;

                            if (nextLine.EndsWith(delimiter))
                                nextLine = nextLine[0..^1];

                            if (string.IsNullOrEmpty(nextLine))
                                continue;

                            line += "\n" + nextLine;
                            lineLength = line.Length;
                            linePos--;
                            continue;
                        }

                        if (quotes > 0 || c != delimiter)
                        {
                            builder.Append(c);
                            if (builderLength == 0 && isQuote)
                                cellStartsWithQuote = true;

                            if (quotes > 0 && isQuote && nextCharIsQuote && builderLength > 0)
                            {
                                linePos++; // Skip next quote. RFC 4180, 2/7: If double-quotes are used to enclose fields, then a double-quote appearing inside a field must be escaped by preceding it with another double quote.
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
                                partList.Add(builder.ToString());

                                builder.Clear();
                                builderLength = 0;
                                cellStartsWithQuote = false;
                            }
                        }
                    }

                    if (firstRow)
                    {
                        firstRow = false;

                        if (HasHeaderRow)
                        {
                            columnNames = partList.ToArray();

                            if (removeSurroundingDoubleQuotes)
                            {
                                for (var i = 0; i < columnNames.Length; i++)
                                {
                                    var columnName = columnNames[i];
                                    if (columnName.Length > 1
                                        && columnName.StartsWith("\"", StringComparison.Ordinal)
                                        && columnName.EndsWith("\"", StringComparison.Ordinal))
                                    {
                                        columnNames[i] = columnName[1..^1];
                                    }
                                }
                            }

                            for (var i = 0; i < columnNames.Length - 1; i++)
                            {
                                var columnName = columnNames[i];
                                for (var j = i + 1; j < columnNames.Length; j++)
                                {
                                    if (string.Equals(columnName, columnNames[j], StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        var message = "delimited input contains more than one columns with the same name: " + columnName;
                                        var exception = new EtlException(this, "error while processing delimited input: " + message);
                                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while processing delimited input: {0}, message: {1}", LineSource.GetType(), message));
                                        exception.Data.Add("Topic", LineSource.GetTopic());

                                        Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, LineSource.GetIoCommandUid(), 0, exception);
                                        throw exception;
                                    }
                                }
                            }

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

                        var valueString = partList[i];

                        object sourceValue = valueString;

                        if (removeSurroundingDoubleQuotes
                           && valueString.Length > 1
                           && valueString.StartsWith("\"", StringComparison.Ordinal)
                           && valueString.EndsWith("\"", StringComparison.Ordinal))
                        {
                            sourceValue = valueString[1..^1];
                        }

                        if (sourceValue != null && treatEmptyStringAsNull && (sourceValue is string str) && string.IsNullOrEmpty(str))
                        {
                            sourceValue = null;
                        }

                        if (columnMap != null && columnMap.TryGetValue(csvColumn, out var columnConfiguration))
                        {
                            var value = columnConfiguration.config.Process(this, sourceValue);
                            initialValues.Add(new KeyValuePair<string, object>(columnConfiguration.rowColumn, value));
                        }
                        else if (DefaultColumns != null)
                        {
                            var value = DefaultColumns.Process(this, sourceValue);
                            initialValues.Add(new KeyValuePair<string, object>(csvColumn, value));
                        }
                    }

                    resultCount++;
                    yield return Context.CreateRow(this, initialValues);
                }
            }
            finally
            {
                LineSource.Release(this);
            }
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class DelimitedFileReaderFluent
    {
        public static IFluentProcessMutatorBuilder ReadDelimitedLines(this IFluentProcessBuilder builder, DelimitedLineReader reader)
        {
            return builder.ReadFrom(reader);
        }
    }
}