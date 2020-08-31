﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;

    public class DelimitedFileReader : AbstractProducer, IRowReader
    {
        public string FileName { get; set; }

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; set; } = true;

        /// <summary>
        /// Default true. If a value starts and ends with double quote (") characters, then both will be removed (this happens before type conversion)
        /// </summary>
        public bool RemoveSurroundingDoubleQuotes { get; set; } = true;

        /// <summary>
        /// Default true. If a value starts with double quote (") character and misses the closing quote, then throw an exception
        /// </summary>
        public bool ThrowOnMissingDoubleQuoteClose { get; set; } = true;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool HasHeaderRow { get; set; }

        /// <summary>
        /// Default null. Column names must be set if <see cref="HasHeaderRow"/> is false, otherwise it should be left null.
        /// </summary>
        public string[] ColumnNames { get; set; }

        /// <summary>
        /// Default value is ';'
        /// </summary>
        public char Delimiter { get; set; } = ';';

        public DelimitedFileReader(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (!HasHeaderRow && (ColumnNames == null || ColumnNames.Length == 0))
                throw new ProcessParameterNullException(this, nameof(ColumnNames));

            if (HasHeaderRow && ColumnNames?.Length > 0)
                throw new InvalidProcessParameterException(this, nameof(ColumnNames), ColumnNames, nameof(ColumnNames) + " must be null if " + nameof(HasHeaderRow) + " is true.");
        }

        protected override IEnumerable<IRow> Produce()
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.fileRead, FileName, null, null, null, null,
                "reading from {FileName}",
                PathHelpers.GetFriendlyPathName(FileName));

            if (!File.Exists(FileName))
            {
                var exception = new ProcessExecutionException(this, "input file doesn't exist");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exist: {0}",
                    FileName));

                exception.Data.Add("FileName", FileName);

                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, 0, exception);
                throw exception;
            }

            var columnConfig = ColumnConfiguration?.ToDictionary(x => x.SourceColumn.ToUpperInvariant(), StringComparer.OrdinalIgnoreCase);
            var resultCount = 0;

            StreamReader reader;
            try
            {
                reader = new StreamReader(FileName);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, null, ex);

                var exception = new EtlException(this, "error while opening file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening file: {0}, message: {1}", FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            var firstRow = true;
            var initialValues = new List<KeyValuePair<string, object>>();

            var partList = new List<string>(100);
            var builder = new StringBuilder(2000);

            // capture for performance
            var columnNames = ColumnNames;
            var delimiter = Delimiter;
            var treatEmptyStringAsNull = TreatEmptyStringAsNull;
            var removeSurroundingDoubleQuotes = RemoveSurroundingDoubleQuotes;
            var throwOnMissingDoubleQuoteClose = ThrowOnMissingDoubleQuoteClose;

            try
            {
                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    string line;
                    try
                    {
                        line = reader.ReadLine();
                        if (line == null)
                            break;

                        if (string.IsNullOrEmpty(line))
                            continue;
                    }
                    catch (Exception ex)
                    {
                        Context.RegisterIoCommandFailed(this, IoCommandKind.fileRead, iocUid, resultCount, ex);
                        var exception = new EtlException(this, "error while reading data from file", ex);
                        exception.Data.Add("FileName", FileName);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from file: {0}, message: {1}", FileName, ex.Message));
                        throw exception;
                    }

                    if (line.EndsWith(delimiter))
                    {
                        line = line[0..^1];
                    }

                    partList.Clear();
                    builder.Clear();

                    var quotes = 0;
                    var builderLength = 0;
                    var builderStartsWithQuote = false;
                    var lineLength = line.Length;

                    for (var linePos = 0; linePos < lineLength; linePos++)
                    {
                        var c = line[linePos];
                        var isQuote = c == '\"';
                        var endOfLine = linePos == lineLength - 1;
                        var nextCharIsQuote = !endOfLine && line[linePos + 1] == delimiter;

                        if (builderLength == 0 && isQuote)
                        {
                            quotes++;
                        }

                        var quotedCellClosing = endOfLine
                            || (builderLength > 0
                                && isQuote
                                && quotes > 0
                                && nextCharIsQuote);

                        if (quotedCellClosing)
                        {
                            quotes--;
                        }

                        var endOfCell = endOfLine || (nextCharIsQuote && quotes == 0);

                        var quotedCellIsNotClosed = builderLength > 0
                            && !isQuote && (endOfLine || c != delimiter)
                            && endOfCell
                            && builderStartsWithQuote;

                        if (quotedCellIsNotClosed && throwOnMissingDoubleQuoteClose)
                        {
                            var exception = new ProcessExecutionException(this, "Cell starting with '\"' is missing closing '\"'.");
                            exception.Data.Add("LineNumber", resultCount + 1);
                            exception.Data.Add("ColumnIndex", linePos + 1);
                            throw exception;
                        }

                        if (c != delimiter || quotes > 0)
                        {
                            builder.Append(c);
                            if (builderLength == 0 && isQuote)
                                builderStartsWithQuote = true;

                            builderLength++;
                        }

                        if (endOfLine || (quotes == 0 && c == delimiter))
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
                                builderStartsWithQuote = false;
                            }
                        }
                    }

                    if (firstRow)
                    {
                        firstRow = false;

                        if (HasHeaderRow)
                        {
                            columnNames = partList.ToArray();
                            continue;
                        }
                    }

                    initialValues.Clear();
                    var colCnt = Math.Min(columnNames.Length, partList.Count);
                    for (var i = 0; i < colCnt; i++)
                    {
                        var columnName = columnNames[i];
                        var valueString = partList[i];

                        object sourceValue = valueString;

                        if (removeSurroundingDoubleQuotes
                           && valueString.Length > 1
                           && valueString.StartsWith("\"", StringComparison.InvariantCultureIgnoreCase)
                           && valueString.EndsWith("\"", StringComparison.InvariantCultureIgnoreCase))
                        {
                            sourceValue = valueString[1..^1];
                        }

                        if (sourceValue != null && treatEmptyStringAsNull && (sourceValue is string str) && string.IsNullOrEmpty(str))
                        {
                            sourceValue = null;
                        }

                        if (columnConfig != null && columnConfig.TryGetValue(columnName, out var columnConfiguration))
                        {
                            var value = HandleConverter(sourceValue, columnConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn, value));
                        }
                        else if (DefaultColumnConfiguration != null)
                        {
                            var value = HandleConverter(sourceValue, DefaultColumnConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(columnName, value));
                        }
                    }

                    resultCount++;
                    yield return Context.CreateRow(this, initialValues);
                }
            }
            finally
            {
                reader.Dispose();
            }

            Context.RegisterIoCommandSuccess(this, IoCommandKind.fileRead, iocUid, resultCount);
        }
    }
}