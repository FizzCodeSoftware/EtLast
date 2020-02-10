namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public delegate T SoapReaderClientCreatorDelegate<T>(SoapReaderProcess<T> process);
    public delegate IEnumerable<Dictionary<string, object>> SoapReaderClientInvokerDelegate<T>(SoapReaderProcess<T> process, T client);

    public class SoapReaderProcess<T> : AbstractProducerProcess, IRowReader
    {
        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; set; } = true;

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        public SoapReaderClientCreatorDelegate<T> ClientCreator { get; set; }
        public SoapReaderClientInvokerDelegate<T> ClientInvoker { get; set; }

        public SoapReaderProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            if (ClientCreator == null)
                throw new ProcessParameterNullException(this, nameof(ClientCreator));

            if (ClientInvoker == null)
                throw new ProcessParameterNullException(this, nameof(ClientInvoker));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }

        protected override IEnumerable<IRow> Produce()
        {
            var startedOn = Stopwatch.StartNew();

            var client = ClientCreator.Invoke(this);
            var result = ClientInvoker.Invoke(this, client);

            CounterCollection.IncrementTimeSpan("SOAP time - success", startedOn.Elapsed);
            CounterCollection.IncrementCounter("SOAP incovations - success", 1);

            var initialValues = new List<KeyValuePair<string, object>>();

            var columnConfig = ColumnConfiguration.ToDictionary(x => x.SourceColumn.ToLowerInvariant());

            foreach (var rowData in result)
            {
                initialValues.Clear();

                foreach (var kvp in rowData)
                {
                    CounterCollection.IncrementCounter("SOAP rows read", 1);

                    var value = kvp.Value;

                    if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                    {
                        value = null;
                    }

                    columnConfig.TryGetValue(kvp.Key.ToLowerInvariant(), out var columnConfiguration);
                    if (columnConfiguration != null)
                    {
                        var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                        value = HandleConverter(value, columnConfiguration);
                        initialValues.Add(new KeyValuePair<string, object>(column, value));
                    }
                    else if (DefaultColumnConfiguration != null)
                    {
                        var column = kvp.Key;
                        value = HandleConverter(value, DefaultColumnConfiguration);
                        initialValues.Add(new KeyValuePair<string, object>(column, value));
                    }

                    var row = Context.CreateRow(this, initialValues);
                    yield return row;
                }
            }
        }
    }
}