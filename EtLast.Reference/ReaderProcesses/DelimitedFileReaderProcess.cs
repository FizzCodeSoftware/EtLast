namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    public class DelimitedFileReaderProcess : AbstractBaseProducerProcess
    {
        public string FileName { get; set; }

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderColumnConfiguration DefaultConfiguration { get; set; }

        public bool TreatEmptyStringAsNull { get; set; }

        public bool HasHeaderRow { get; set; }
        public string[] ColumnNames { get; set; }

        /// <summary>
        /// Default value is ';'
        /// </summary>
        public char Delimiter { get; set; } = ';';

        public DelimitedFileReaderProcess(IEtlContext context, string name)
            : base(context, name)
        { }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName)) throw new ProcessParameterNullException(this, nameof(FileName));
            if (!HasHeaderRow && (ColumnNames == null || ColumnNames.Length == 0)) throw new ProcessParameterNullException(this, nameof(ColumnNames));

            var sw = Stopwatch.StartNew();

            var rows = EvaluateInputProcess(sw);
            foreach (var row in rows)
            {
                yield return row;
            }

            Context.Log(LogSeverity.Information, this, "reading from {FileName}", FileName);

            var resultCount = 0;

            using (var reader = new StreamReader(FileName))
            {
                var columnNames = ColumnNames;
                string line;
                var rowNumber = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.EndsWith(Delimiter.ToString()))
                    {
                        line = line.Substring(0, line.Length - 1);
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
                        if (valueString.StartsWith("\"") && valueString.EndsWith("\""))
                        {
                            valueString = valueString.Substring(1, valueString.Length - 2);
                        }

                        object value = valueString;

                        if (value != null && TreatEmptyStringAsNull && (value is string str) && str == string.Empty)
                        {
                            value = null;
                        }

                        var columnConfiguration = ColumnConfiguration.Find(x => string.Compare(x.SourceColumn, columnNames[i], true) == 0);

                        if (columnConfiguration != null || DefaultConfiguration == null)
                        {
                            var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                            value = ReaderProcessHelper.HandleConverter(this, value, rowNumber, column, columnConfiguration, row, out var error);
                            if (error) continue;

                            row.SetValue(column, value, this);
                        }
                        else
                        {
                            var column = columnNames[i];
                            value = ReaderProcessHelper.HandleConverter(this, value, rowNumber, column, DefaultConfiguration, row, out var error);
                            if (error) continue;

                            row.SetValue(column, value, this);
                        }
                    }

                    rowNumber++;
                    resultCount++;
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }
    }
}