namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;

    public class DelimitedFileReaderProcess : AbstractBaseProducerProcess
    {
        public string FileName { get; set; }

        public List<(string SourceColumnName, string RowColumnName, ITypeConverter Converter, object ValueIfNull)> ColumnMap { get; set; }
        public ITypeConverter DefaultConverter { get; set; }
        public object DefaultValueIfNull { get; set; }

        public bool HasHeaderRow { get; set; }
        public char Delimiter { get; set; } = ';';

        public DelimitedFileReaderProcess(IEtlContext context, string name)
            : base(context, name)
        { }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(FileName)) throw new ProcessParameterNullException(this, nameof(FileName));

            var sw = Stopwatch.StartNew();

            foreach (var row in EvaluateInputProcess(sw))
                yield return row;

            Context.Log(LogSeverity.Information, this, "reading from {FileName}", FileName);

            var resultCount = 0;

            using (var reader = new StreamReader(FileName))
            {
                string[] fileColumnNames = null;
                string line;
                int rowNumber = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.EndsWith(Delimiter.ToString()))
                        line = line.Substring(0, line.Length - 1);

                    string[] array = line.Split(Delimiter);
                    if (rowNumber == 0 && HasHeaderRow)
                    {
                        List<string> headers = new List<string>(array.Length);
                        foreach (var header in array)
                            headers.Add(header);
                        fileColumnNames = headers.ToArray();
                        rowNumber++;
                        continue;
                    }
                    if(rowNumber == 0 && !HasHeaderRow)
                        // TODO default column names
                        fileColumnNames = new string[] { "Column1", "Column2" };

                    //var row = Context.CreateRow(reader.FieldCount);
                    var row = Context.CreateRow(array.Length);

                    for (int i = 0; i < array.Length; i++)
                    {
                        string valueString = array[i];
                        if (valueString.StartsWith("\"") && valueString.EndsWith("\""))
                            valueString = valueString.Substring(1, valueString.Length - 2);

                        object value = valueString;

                        // map excel column name to ColumnMap name, and convert
                        var givenColumn = ColumnMap.FirstOrDefault(x => string.Compare(x.SourceColumnName, fileColumnNames[i], true) == 0);

                        if (givenColumn.RowColumnName == null && DefaultConverter != null)
                        {
                            givenColumn.Converter = DefaultConverter;
                            givenColumn.ValueIfNull = DefaultValueIfNull;
                        }

                        value = ReaderProcessHelper.HandleConverter(this, value, givenColumn.RowColumnName, (rowNumber, givenColumn.Converter, givenColumn.ValueIfNull), row, out bool shouldContinue);
                        if (shouldContinue)
                            continue;

                        row[givenColumn.RowColumnName] = value;
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
