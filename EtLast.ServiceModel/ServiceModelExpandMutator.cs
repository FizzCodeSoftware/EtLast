namespace FizzCode.EtLast;

public delegate TClient ServiceModelExpandMutatorClientCreatorDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row)
    where TChannel : class
    where TClient : ClientBase<TChannel>;

public delegate object ServiceModelExpandMutatorClientInvokerDelegate<TChannel, TClient>(ServiceModelExpandMutator<TChannel, TClient> process, IReadOnlySlimRow row, TClient client)
    where TChannel : class
    where TClient : ClientBase<TChannel>;

public sealed class ServiceModelExpandMutator<TChannel, TClient>: AbstractMutator
    where TChannel : class
    where TClient : ClientBase<TChannel>
{
    [ProcessParameterMustHaveValue]
    public required ServiceModelExpandMutatorClientCreatorDelegate<TChannel, TClient> ClientCreator { get; init; }

    [ProcessParameterMustHaveValue]
    public required ServiceModelExpandMutatorClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; init; }

    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

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

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
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

            var iocUid = Context.RegisterIoCommandStartWithLocation(this, IoCommandKind.serviceRead, _client.Endpoint.Address.ToString(), Convert.ToInt32(_client.InnerChannel.OperationTimeout.TotalSeconds), null, null, null,
                "sending SOAP request", null);
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
                _client = default;

                var exception = new EtlException(this, "error while reading data from service", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from service: {0}",
                    _client.Endpoint.Address.ToString()));

                exception.Data["EndpointAddress"] = _client.Endpoint.Address.ToString();

                Context.RegisterIoCommandFailed(this, IoCommandKind.serviceRead, iocUid, null, exception);
                throw exception;
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
                    row[TargetColumn] = new EtlRowError(this, null, "SOAP invocation failed");
                    break;
            }
        }

        if (!removeRow)
            yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ServiceModelExpandMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ExpandWithServiceResponse<TChannel, TClient>(this IFluentSequenceMutatorBuilder builder, ServiceModelExpandMutator<TChannel, TClient> mutator)
        where TChannel : class
        where TClient : ClientBase<TChannel>
    {
        return builder.AddMutator(mutator);
    }
}
