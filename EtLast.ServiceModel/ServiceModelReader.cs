namespace FizzCode.EtLast;

public delegate TClient ServiceModelReaderClientCreatorDelegate<TChannel, TClient>(ServiceModelReader<TChannel, TClient> process)
    where TChannel : class
    where TClient : ClientBase<TChannel>;

public delegate IEnumerable<SlimRow> ServiceModelReaderClientInvokerDelegate<TChannel, TClient>(ServiceModelReader<TChannel, TClient> process, TClient client)
    where TChannel : class
    where TClient : ClientBase<TChannel>;

public sealed class ServiceModelReader<TChannel, TClient> : AbstractRowSource
    where TChannel : class
    where TClient : ClientBase<TChannel>
{
    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool TreatEmptyStringAsNull { get; init; } = true;

    public Dictionary<string, ReaderColumn> Columns { get; init; }
    public ReaderDefaultColumn DefaultColumns { get; init; }

    public ServiceModelReaderClientCreatorDelegate<TChannel, TClient> ClientCreator { get; init; }
    public ServiceModelReaderClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; init; }

    public ServiceModelReader(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (ClientCreator == null)
            throw new ProcessParameterNullException(this, nameof(ClientCreator));

        if (ClientInvoker == null)
            throw new ProcessParameterNullException(this, nameof(ClientInvoker));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
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
            var exception = new EtlException(this, "error while reading data from service", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from service: {0}",
                client.Endpoint.Address.ToString()));
            exception.Data["EndpointAddress"] = client.Endpoint.Address.ToString();

            Context.RegisterIoCommandFailed(this, IoCommandKind.serviceRead, iocUid, null, exception);
            throw exception;
        }

        var resultCount = 0;
        if (enumerator != null && !Pipe.IsTerminating)
        {
            var initialValues = new Dictionary<string, object>();

            // key is the SOURCE column name
            var columnMap = Columns?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

            while (!Pipe.IsTerminating)
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
                    exception.Data["Endpoint"] = client.Endpoint.Address.ToString();
                    throw exception;
                }

                var rowData = enumerator.Current;

                initialValues.Clear();

                foreach (var valueKvp in rowData.Values)
                {
                    var column = valueKvp.Key;
                    var value = valueKvp.Value;

                    if (value != null && TreatEmptyStringAsNull && (value is string str) && string.IsNullOrEmpty(str))
                        value = null;

                    if (columnMap.TryGetValue(column, out var col))
                    {
                        value = col.config.Process(this, value);
                        initialValues[col.rowColumn] = value;
                    }
                    else if (DefaultColumns != null)
                    {
                        value = DefaultColumns.Process(this, value);
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

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ServiceModelReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromService<TChannel, TClient>(this IFluentSequenceBuilder builder, ServiceModelReader<TChannel, TClient> reader)
        where TChannel : class
        where TClient : ClientBase<TChannel>
    {
        return builder.ReadFrom(reader);
    }
}
