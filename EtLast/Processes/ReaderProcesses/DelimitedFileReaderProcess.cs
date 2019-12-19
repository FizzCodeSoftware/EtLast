namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;

    public class DelimitedFileReaderProcess : AbstractProducerProcess
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

        public override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (!HasHeaderRow && (ColumnNames == null || ColumnNames.Length == 0))
                throw new ProcessParameterNullException(this, nameof(ColumnNames));
        }

        protected override IEnumerable<IRow> Produce()
        {
            if (!File.Exists(FileName))
            {
                var exception = new EtlException(this, "input file doesn't exists");
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "input file doesn't exists: {0}", FileName));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            Context.Log(LogSeverity.Debug, this, "reading from {FileName}", PathHelpers.GetFriendlyPathName(FileName));

            using (var reader = new StreamReader(FileName))
            {
                var columnNames = ColumnNames;
                string line;
                var firstRow = true;
                var initialValues = new List<KeyValuePair<string, object>>();
                while ((line = reader.ReadLine()) != null && !Context.CancellationTokenSource.IsCancellationRequested)
                {
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

                        var columnConfiguration = ColumnConfiguration.Find(x => string.Equals(x.SourceColumn, columnNames[i], StringComparison.InvariantCultureIgnoreCase));

                        if (columnConfiguration != null || DefaultConfiguration == null)
                        {
                            var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                            value = HandleConverter(value, columnConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(column, value));
                        }
                        else
                        {
                            var column = columnNames[i];
                            value = HandleConverter(value, DefaultConfiguration);
                            initialValues.Add(new KeyValuePair<string, object>(column, value));
                        }
                    }

                    var row = Context.CreateRow(this, initialValues);
                    yield return row;
                }
            }
        }
    }
}