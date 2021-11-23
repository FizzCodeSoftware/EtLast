namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.ServiceModel;

    public delegate TClient ServiceModelExpandMutatorClientCreatorDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public delegate object ServiceModelExpandMutatorClientInvokerDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row, TClient client)
        where TChannel : class
        where TClient : ClientBase<TChannel>;

    public sealed class ServiceModelExpandMutator<TChannel, TClient> : AbstractMutator
        where TChannel : class
        where TClient : ClientBase<TChannel>
    {
        public ServiceModelExpandMutatorClientCreatorDelegate<TChannel, TClient> ClientCreator { get; init; }
        public ServiceModelExpandMutatorClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; init; }
        public string TargetColumn { get; init; }

        /// <summary>
        /// Default value is <see cref="InvalidValueAction.Keep"/>
        /// </summary>
        public InvalidValueAction ActionIfFailed { get; init; } = InvalidValueAction.Keep;

        public object SpecialValueIfFailed { get; init; }

        private TClient _client;

        /// <summary>
        /// Default value is 5.
        /// </summary>
        public int MaxRetryCount { get; init; } = 5;

        public ServiceModelExpandMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var success = false;
            for (var retryCount = 0; retryCount <= MaxRetryCount; retryCount++)
            {
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
                    _client = default;
                    continue;
                }

                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.serviceRead, _client.Endpoint.Address.ToString(), Convert.ToInt32(_client.InnerChannel.OperationTimeout.TotalSeconds), null, null, null,
                    "sending request to {EndpointAddress}",
                    _client.Endpoint.Address.ToString());
                try
                {
                    var result = ClientInvoker.Invoke(this, row, _client);
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.serviceRead, iocUid, null);

                    if (result != null)
                    {
                        row[TargetColumn] = result;
                    }

                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.serviceRead, iocUid, null, ex);
                    _client = default;
                }
            }

            var removeRow = false;
            if (!success)
            {
                switch (ActionIfFailed)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row[TargetColumn] = SpecialValueIfFailed;
                        break;
                    case InvalidValueAction.Throw:
                        throw new ProcessExecutionException(this, row, "SOAP invocation failed");
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    case InvalidValueAction.WrapError:
                        row[TargetColumn] = new EtlRowError
                        {
                            Process = this,
                            OriginalValue = null,
                            Message = "SOAP invocation failed",
                        };
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ServiceModelExpandMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ExpandWithServiceResponse<TChannel, TClient>(this IFluentProcessMutatorBuilder builder, ServiceModelExpandMutator<TChannel, TClient> mutator)
            where TChannel : class
            where TClient : ClientBase<TChannel>
        {
            return builder.AddMutator(mutator);
        }
    }
}