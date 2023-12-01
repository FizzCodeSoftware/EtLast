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
    public required ServiceModelReaderClientCreatorDelegate<TChannel, TClient> ClientCreator { get; init; }
    public required ServiceModelReaderClientInvokerDelegate<TChannel, TClient> ClientInvoker { get; init; }
    public required Dictionary<string, ReaderColumn> Columns { get; init; }

    public ReaderColumn DefaultColumns { get; init; }

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool TreatEmptyStringAsNull { get; init; } = true;

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

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.serviceRead,
            Location = client.Endpoint.Address.ToString(),
            TimeoutSeconds = Convert.ToInt32(client.InnerChannel.OperationTimeout.TotalSeconds),
            Message = "sending SOAP request"
        });

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

            ioCommand.Failed(exception);
            throw exception;
        }

        var resultCount = 0;
        if (enumerator != null && !FlowState.IsTerminating)
        {
            var initialValues = new Dictionary<string, object>();

            // key is the SOURCE column name
            var columnMap = Columns?.ToDictionary(kvp => kvp.Value.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

            while (!FlowState.IsTerminating)
            {
                try
                {
                    if (!enumerator.MoveNext())
                        break;
                }
                catch (Exception ex)
                {
                    var exception = new EtlException(this, "error while reading data from service", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading data from service: {0}", client.Endpoint.Address.ToString()));
                    exception.Data["Endpoint"] = client.Endpoint.Address.ToString();

                    ioCommand.AffectedDataCount += resultCount;
                    ioCommand.End();
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
                        try
                        {
                            initialValues[col.rowColumn] = col.config.Process(this, value);
                        }
                        catch (Exception ex)
                        {
                            initialValues[col.rowColumn] = new EtlRowError(this, value, ex);
                        }
                    }
                    else if (DefaultColumns != null)
                    {
                        try
                        {
                            initialValues[column] = DefaultColumns.Process(this, value);
                        }
                        catch (Exception ex)
                        {
                            initialValues[column] = new EtlRowError(this, value, ex);
                        }
                    }
                }

                resultCount++;
                yield return Context.CreateRow(this, initialValues);
            }
        }

        ioCommand.AffectedDataCount += resultCount;
        ioCommand.End();
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
