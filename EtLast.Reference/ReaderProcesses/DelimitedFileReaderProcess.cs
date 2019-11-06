namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;

    public class DelimitedFileReaderProcess : AbstractBaseProducerProcess
    {
        public string FileName { get; set; }

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultConfiguration { get; set; }

        public bool TreatEmptyStringAsNull { get; set; }

        public bool HasHeaderRow { get; set; }
        public string[] ColumnNames { get; set; }

        /// <summary>
        /// Default value is ';'
        /// </summary>
        public char Delimiter { get; set; } = ';';

        public DelimitedFileReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IExecutionBlock caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (!HasHeaderRow && (ColumnNames == null || ColumnNames.Length == 0))
                throw new ProcessParameterNullException(this, nameof(ColumnNames));

            var startedOn = Stopwatch.StartNew();

            var rows = EvaluateInputProcess(startedOn);
            foreach (var row in rows)
            {
                yield return row;
            }

            if (!File.Exists(FileName))
            {
                var exception = new EtlException(this, "input file doesn't exists");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exists: {0}", FileName));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            Context.Log(LogSeverity.Debug, this, "reading from {FileName}", PathHelpers.GetFriendlyPathName(FileName));

            var resultCount = 0;

            using (var reader = new StreamReader(FileName))
            {
                var columnNames = ColumnNames;
                string line;
                var rowNumber = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.EndsWith(Delimiter))
                    {
                        line = line[0..^1];
                    }

                    var parts = line.Split(Delimiter);
                    if (rowNumber == 0 && HasHeaderRow)
                    {
                        columnNames = parts;
                        rowNumber++;
                        continue;
                    }

                    var row = Context.CreateRow(parts.Length);
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

                        var columnConfiguration = ColumnConfiguration.Find(x => string.Equals(x.SourceColumn, columnNames[i], StringComparison.InvariantCultureIgnoreCase));

                        if (columnConfiguration != null || DefaultConfiguration == null)
                        {
                            var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                            value = ReaderProcessHelper.HandleConverter(this, value, column, columnConfiguration, row, out var error);
                            if (error)
                                continue;

                            row.SetValue(column, value, this);
                        }
                        else
                        {
                            var column = columnNames[i];
                            value = ReaderProcessHelper.HandleConverter(this, value, column, DefaultConfiguration, row, out var error);
                            if (error)
                                continue;

                            row.SetValue(column, value, this);
                        }
                    }

                    if (IgnoreRowsWithError && row.HasError())
                        continue;

                    rowNumber++;
                    resultCount++;
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, startedOn.Elapsed);
        }
    }
}