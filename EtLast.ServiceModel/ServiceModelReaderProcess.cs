namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.ServiceModel;

    public delegate TClient SoapReaderClientCreatorDelegate<TChannel, TClient>(ServiceModelReaderProcess<TChannel, TClient> process)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public delegate IEnumerable<SlimRow> SoapReaderClientInvokerDelegate<TChannel, TClient>(ServiceModelReaderProcess<TChannel, TClient> process, TClient client)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public class ServiceModelReaderProcess<TChannel, TClient> : AbstractProducer, IRowReader
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
            var client = ClientCreator.Invoke(this);

            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.serviceRead, client.Endpoint.Address.ToString(), Convert.ToInt32(client.InnerChannel.OperationTimeout.TotalSeconds), null, null, null,
                "sending request to {EndpointAddress}",
                client.Endpoint.Address.ToString());

            IEnumerator<SlimRow> enumerator;
            try
            {
                enumerator = ClientInvoker.Invoke(this, client).GetEnumerator();
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.serviceRead, iocUid, null, ex);
                var exception = new EtlException(this, "error while reading data from service", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from service: {0}", client.Endpoint.Address.ToString()));
                exception.Data.Add("Endpoint", client.Endpoint.Address.ToString());
                throw exception;
            }

            var resultCount = 0;
            if (enumerator != null && !Context.CancellationTokenSource.IsCancellationRequested)
            {
                var initialValues = new Dictionary<string, object>();
                var columnMap = ColumnConfiguration.ToDictionary(x => x.SourceColumn.ToUpperInvariant());
                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        if (!enumerator.MoveNext())
                            break;
                    }
                    catch (Exception ex)
                    {
                        Context.RegisterIoCommandFailed(this, IoCommandKind.serviceRead, iocUid, resultCount, ex);
                        var exception = new EtlException(this, "error while reading data from service", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from service: {0}", client.Endpoint.Address.ToString()));
                        exception.Data.Add("Endpoint", client.Endpoint.Address.ToString());
                        throw exception;
                    }

                    var rowData = enumerator.Current;

                    initialValues.Clear();

                    foreach (var kvp in rowData.Values)
                    {
                        var value = kvp.Value;

                        if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                        {
                            value = null;
                        }

                        columnMap.TryGetValue(kvp.Key.ToUpperInvariant(), out var columnConfiguration);
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

                    resultCount++;
                    yield return Context.CreateRow(this, initialValues);
                }
            }

            Context.RegisterIoCommandSuccess(this, IoCommandKind.serviceRead, iocUid, resultCount);
        }
    }
}