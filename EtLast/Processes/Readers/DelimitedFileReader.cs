namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;

    public class DelimitedFileReader : AbstractProducer, IRowReader
    {
        public string FileName { get; set; }

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; set; } = true;

        public bool HasHeaderRow { get; set; }
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

                Context.RegisterIoCommandFailed(this, iocUid, 0, exception);
                throw exception;
            }

            var columnConfig = ColumnConfiguration.ToDictionary(x => x.SourceColumn.ToUpperInvariant());
            var resultCount = 0;

            StreamReader reader;
            try
            {
                reader = new StreamReader(FileName);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, 0, ex);

                var exception = new EtlException(this, "error while opening file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening file: {0}, message: {1}", FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            var columnNames = ColumnNames;
            var firstRow = true;
            var initialValues = new List<KeyValuePair<string, object>>();

            try
            {
                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    string line;
                    try
                    {
                        line = reader.ReadLine();
                        if (string.IsNullOrEmpty(line))
                            break;
                    }
                    catch (Exception ex)
                    {
                        Context.RegisterIoCommandFailed(this, iocUid, resultCount, ex);
                        var exception = new EtlException(this, "error while reading data from file", ex);
                        exception.Data.Add("FileName", FileName);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from file: {0}, message: {1}", FileName, ex.Message));
                        throw exception;
                    }

                    if (line.EndsWith(Delimiter))
                    {
                        line = line[0..^1];
                    }

                    var parts = line.Split(Delimiter);
                    if (firstRow)
                    {
                        firstRow = false;

                        if (HasHeaderRow)
                        {
                            columnNames = parts;
                            continue;
                        }
                    }

                    initialValues.Clear();
                    for (var i = 0; i < parts.Length; i++)
                    {
                        var valueString = parts[i];
                        if (valueString.StartsWith("\"", StringComparison.InvariantCultureIgnoreCase) && valueString.EndsWith("\"", StringComparison.InvariantCultureIgnoreCase))
                        {
                            valueString = valueString[1..^1];
                        }

                        object value = valueString;

                        if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                        {
                            value = null;
                        }

                        columnConfig.TryGetValue(columnNames[i].ToUpperInvariant(), out var columnConfiguration);
                        if (columnConfiguration != null)
                        {
                            var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                            value = HandleConverter(value, columnConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(column, value));
                        }
                        else if (DefaultColumnConfiguration != null)
                        {
                            var column = columnNames[i];
                            value = HandleConverter(value, DefaultColumnConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(column, value));
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

            Context.RegisterIoCommandSuccess(this, iocUid, resultCount);
        }
    }
}