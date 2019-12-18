namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public delegate T SoapExpanderClientCreatorDelegate<T>(SoapExpandOperation<T> op, IRow row);
    public delegate object SoapExpanderClientInvokerDelegate<T>(SoapExpandOperation<T> op, IRow row, T client);

    public class SoapExpandOperation<T> : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public SoapExpanderClientCreatorDelegate<T> ClientCreator { get; set; }
        public SoapExpanderClientInvokerDelegate<T> ClientInvoker { get; set; }
        public string TargetColumn { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        private T _client;

        /// <summary>
        /// Default value is 5.
        /// </summary>
        public int MaxRetryCount { get; set; } = 5;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            var startedOn = Stopwatch.StartNew();

            for (var retryCount = 0; retryCount <= MaxRetryCount; retryCount++)
            {
                startedOn.Restart();

                try
                {
                    if (_client == null)
                    {
                        _client = ClientCreator.Invoke(this, row);
                    }

                    if (_client != null)
                    {
                        var result = ClientInvoker.Invoke(this, row, _client);
                        if (result != null)
                        {
                            row.SetValue(TargetColumn, result, this);
                        }

                        CounterCollection.IncrementTimeSpan("SOAP time - success", startedOn.Elapsed);
                        CounterCollection.IncrementCounter("SOAP requests - success", 1);
                        return;
                    }
                }
                catch (Exception)
                {
                    CounterCollection.IncrementTimeSpan("SOAP time - failure", startedOn.Elapsed);
                    CounterCollection.IncrementCounter("SOAP requests - failure", 1);
                    _client = default;
                }
            }

            switch (ActionIfFailed)
            {
                case InvalidValueAction.SetSpecialValue:
                    row.SetValue(TargetColumn, SpecialValueIfFailed, this);
                    break;
                case InvalidValueAction.Throw:
                    throw new OperationExecutionException(Process, this, row, "SOAP invocation failed");
                case InvalidValueAction.RemoveRow:
                    Process.RemoveRow(row, this);
                    return;
                case InvalidValueAction.WrapError:
                    row.SetValue(TargetColumn, new EtlRowError
                    {
                        Process = Process,
                        Operation = this,
                        OriginalValue = null,
                        Message = "SOAP invocation failed",
                    }, this);
                    break;
            }
        }

        public override void Prepare()
        {
            if (ClientCreator == null)
                throw new OperationParameterNullException(this, nameof(ClientCreator));

            if (ClientInvoker == null)
                throw new OperationParameterNullException(this, nameof(ClientInvoker));

            if (string.IsNullOrEmpty(TargetColumn))
                throw new OperationParameterNullException(this, nameof(TargetColumn));
        }
    }
}