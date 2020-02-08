namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate T SoapExpanderClientCreatorDelegate<T>(SoapExpandMutator<T> op, IRow row);
    public delegate object SoapExpanderClientInvokerDelegate<T>(SoapExpandMutator<T> op, IRow row, T client);

    public class SoapExpandMutator<T> : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

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

        public SoapExpandMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);

            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

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
                        }

                        if (_client != null)
                        {
                            var result = ClientInvoker.Invoke(this, row, _client);
                            if (result != null)
                            {
                                row.SetValue(TargetColumn, result, this);
                            }

                            CounterCollection.IncrementTimeSpan("SOAP time - success", startedOn.Elapsed);
                            CounterCollection.IncrementCounter("SOAP incovations - success", 1);
                            success = true;
                            break;
                        }
                    }
                    catch (Exception)
                    {
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
                            row.SetValue(TargetColumn, SpecialValueIfFailed, this);
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
                            }, this);
                            break;
                    }
                }

                if (removeRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (ClientCreator == null)
                throw new ProcessParameterNullException(this, nameof(ClientCreator));

            if (ClientInvoker == null)
                throw new ProcessParameterNullException(this, nameof(ClientInvoker));

            if (string.IsNullOrEmpty(TargetColumn))
                throw new ProcessParameterNullException(this, nameof(TargetColumn));
        }
    }
}