namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.ServiceModel;

    public delegate TClient SoapReaderClientCreatorDelegate<TChannel, TClient>(ServiceModelReaderProcess<TChannel, TClient> process)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public delegate IEnumerable<Dictionary<string, object>> SoapReaderClientInvokerDelegate<TChannel, TClient>(ServiceModelReaderProcess<TChannel, TClient> process, TClient client)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public class ServiceModelReaderProcess<TChannel, TClient> : AbstractProducerProcess, IRowReader
        where TChannel : class
        where TClient : ClientBase<TChannel>
    {
        /// <summary>
        /// Default true.
        /// </summary>
        public bool TreatEmptyStringAsNull { get; set; } = true;

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        public SoapReaderClientCreatorDelegate<TChannel, TClient> ClientCreator { get; set; }
        public SoapReaderClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; set; }

        public ServiceModelReaderProcess(ITopic topic, string name)
            : base(topic, name)
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

            Context.Log(LogSeverity.Debug, this, "SOAP client created, endpoint address: {EndpointAddress}",
                client.Endpoint.Address);

            Context.Log(LogSeverity.Debug, this, "sending SOAP request, endpoint address: {EndpointAddress}",
                client.Endpoint.Address);

            var result = ClientInvoker.Invoke(this, client);

            CounterCollection.IncrementTimeSpan("SOAP time - success", startedOn.Elapsed);
            CounterCollection.IncrementCounter("SOAP incovations - success", 1);

            var initialValues = new Dictionary<string, object>();
            var columnConfig = ColumnConfiguration.ToDictionary(x => x.SourceColumn.ToUpperInvariant());
            foreach (var rowData in result)
            {
                initialValues.Clear();
                CounterCollection.IncrementCounter("SOAP rows read", 1);

                foreach (var kvp in rowData)
                {
                    var value = kvp.Value;

                    if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                    {
                        value = null;
                    }

                    columnConfig.TryGetValue(kvp.Key.ToUpperInvariant(), out var columnConfiguration);
                    if (columnConfiguration != null)
                    {
                        var column = columnConfiguration.RowColumn ?? columnConfiguration.SourceColumn;
                        value = HandleConverter(value, columnConfiguration);
                        initialValues[column] = value;
                    }
                    else if (DefaultColumnConfiguration != null)
                    {
                        var column = kvp.Key;
                        value = HandleConverter(value, DefaultColumnConfiguration);
                        initialValues[column] = value;
                    }
                }

                yield return Context.CreateRow(this, initialValues);
            }
        }
    }
}