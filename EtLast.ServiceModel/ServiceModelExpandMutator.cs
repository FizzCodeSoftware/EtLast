namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.ServiceModel;

    public delegate TClient SoapExpanderClientCreatorDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public delegate object SoapExpanderClientInvokerDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row, TClient client)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public class ServiceModelExpandMutator<TChannel, TClient> : AbstractMutator
        where TChannel : class
        where TClient : ClientBase<TChannel>
    {
        public SoapExpanderClientCreatorDelegate<TChannel, TClient> ClientCreator { get; set; }
        public SoapExpanderClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; set; }
        public string TargetColumn { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        private TClient _client;

        /// <summary>
        /// Default value is 5.
        /// </summary>
        public int MaxRetryCount { get; set; } = 5;

        public ServiceModelExpandMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var startedOn = Stopwatch.StartNew();

            var success = false;
            for (var retryCount = 0; retryCount <= MaxRetryCount; retryCount++)
            {
                startedOn.Restart();

                try
                {
                    if (_client == null)
                    {
                        _client = ClientCreator.Invoke(this, row);

                        if (_client != null)
                        {
                            Context.Log(LogSeverity.Debug, this, "SOAP client created, endpoint address: {EndpointAddress}",
                                _client.Endpoint.Address.ToString());
                        }
                    }
                }
                catch (Exception)
                {
                    CounterCollection.IncrementTimeSpan("SOAP time - failure", startedOn.Elapsed);
                    CounterCollection.IncrementCounter("SOAP incovations - failure", 1);
                    _client = default;
                    continue;
                }

                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.serviceRead, _client.Endpoint.Address.ToString(), Convert.ToInt32(_client.InnerChannel.OperationTimeout.TotalSeconds), null, null, null,
                    "sending request to {EndpointAddress}",
                    _client.Endpoint.Address.ToString());
                try
                {
                    var result = ClientInvoker.Invoke(this, row, _client);
                    Context.RegisterIoCommandSuccess(this, iocUid, 0);

                    if (result != null)
                    {
                        row.SetValue(TargetColumn, result);
                    }

                    CounterCollection.IncrementTimeSpan("SOAP time - success", startedOn.Elapsed);
                    CounterCollection.IncrementCounter("SOAP incovations - success", 1);
                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, iocUid, 0, ex);
                    CounterCollection.IncrementTimeSpan("SOAP time - failure", startedOn.Elapsed);
                    CounterCollection.IncrementCounter("SOAP incovations - failure", 1);
                    _client = default;
                }
            }

            var removeRow = false;
            if (!success)
            {
                switch (ActionIfFailed)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row.SetValue(TargetColumn, SpecialValueIfFailed);
                        break;
                    case InvalidValueAction.Throw:
                        throw new ProcessExecutionException(this, row, "SOAP invocation failed");
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    case InvalidValueAction.WrapError:
                        row.SetValue(TargetColumn, new EtlRowError
                        {
                            Process = this,
                            OriginalValue = null,
                            Message = "SOAP invocation failed",
                        });
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            if (ClientCreator == null)
                throw new ProcessParameterNullException(this, nameof(ClientCreator));

            if (ClientInvoker == null)
                throw new ProcessParameterNullException(this, nameof(ClientInvoker));

            if (string.IsNullOrEmpty(TargetColumn))
                throw new ProcessParameterNullException(this, nameof(TargetColumn));
        }
    }
}