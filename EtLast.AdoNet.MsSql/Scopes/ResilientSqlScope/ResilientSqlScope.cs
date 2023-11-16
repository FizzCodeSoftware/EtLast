namespace FizzCode.EtLast;

public enum ResilientSqlScopeTempTableMode
{
    KeepOnlyOnFailure, AlwaysKeep, AlwaysDrop
}

public sealed partial class ResilientSqlScope(IEtlContext context) : AbstractJob(context), IScope
{
    /// <summary>
    /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.RequiresNew"/>.
    /// </summary>
    public TransactionScopeKind InitializationTransactionScopeKind { get; init; } = TransactionScopeKind.RequiresNew;

    /// <summary>
    /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.RequiresNew"/>.
    /// </summary>
    public TransactionScopeKind FinalizerTransactionScopeKind { get; init; } = TransactionScopeKind.RequiresNew;

    /// <summary>
    /// The number of retries of finalizers. Default value is 3. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
    /// </summary>
    public int FinalizerRetryCount { get; init; } = 3;

    /// <summary>
    /// Default value is <see cref="ResilientSqlScopeTempTableMode.KeepOnlyOnFailure"/>.
    /// </summary>
    public ResilientSqlScopeTempTableMode TempTableMode { get; init; } = ResilientSqlScopeTempTableMode.KeepOnlyOnFailure;

    public NamedConnectionString ConnectionString { get; init; }

    /// <summary>
    /// Allows the execution of initializers BEFORE the individual table processes are created and executed.
    /// </summary>
    public Action<ResilientSqlScopeProcessBuilder> Initializers { get; init; }

    /// <summary>
    /// Allows the execution of global finalizers BEFORE the individual table finalizers are created and executed.
    /// </summary>
    public Action<ResilientSqlScopeProcessBuilder> PreFinalizers { get; init; }

    /// <summary>
    /// Allows the execution of global finalizers AFTER the individual table finalizers are created and executed.
    /// </summary>
    public Action<ResilientSqlScopeProcessBuilder> PostFinalizers { get; init; }

    private List<ResilientTable> _tables;
    public List<ResilientTable> Tables
    {
        get => _tables;
        init
        {
            _tables = value;
            foreach (var table in value)
            {
                table.Scope = this;
                if (table.AdditionalTables != null)
                {
                    foreach (var additionalTable in table.AdditionalTables)
                        additionalTable.Scope = this;
                }
            }
        }
    }

    /// <summary>
    /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is "__".
    /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
    /// </summary>
    public string AutoTempTablePrefix { get; init; } = "__";

    /// <summary>
    /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
    /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
    /// </summary>
    public string AutoTempTablePostfix { get; init; }

    public AdditionalData AdditionalData { get; init; }

    public override void ValidateParameters()
    {
        if (Tables == null)
            throw new ProcessParameterNullException(this, nameof(Tables));

        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        if (FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && FinalizerRetryCount > 0)
            throw new InvalidProcessParameterException(this, nameof(FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));

        foreach (var table in Tables)
        {
            if (string.IsNullOrEmpty(table.TempTableName)
                && string.IsNullOrEmpty(AutoTempTablePrefix)
                && string.IsNullOrEmpty(AutoTempTablePostfix))
            {
                throw new InvalidProcessParameterException(this, nameof(table.TempTableName), null, nameof(ResilientTable) + "." + nameof(ResilientTableBase.TempTableName) + " must be specified if there is no " + nameof(AutoTempTablePrefix) + " or " + nameof(AutoTempTablePostfix) + " specified (table name: " + table.TableName + ")");
            }

            if (string.IsNullOrEmpty(table.TableName))
                throw new ProcessParameterNullException(this, nameof(ResilientTableBase.TableName));

            if (table.JobCreator == null && table.PartitionedProducerCreator == null)
                throw new InvalidProcessParameterException(this, nameof(ResilientTable.JobCreator) + "/" + nameof(ResilientTable.PartitionedProducerCreator), null, nameof(ResilientTable.JobCreator) + " or " + nameof(ResilientTable.PartitionedProducerCreator) + " must be supplied for " + table.TableName);

            if (table.JobCreator != null && table.PartitionedProducerCreator != null)
                throw new InvalidProcessParameterException(this, nameof(ResilientTable.JobCreator) + "/" + nameof(ResilientTable.PartitionedProducerCreator), null, "only one of " + nameof(ResilientTable.JobCreator) + " or " + nameof(ResilientTable.PartitionedProducerCreator) + " can be supplied for " + table.TableName);

            if (table.Finalizers == null)
                throw new ProcessParameterNullException(this, nameof(ResilientTable.Finalizers));

            if (table.AdditionalTables != null)
            {
                foreach (var additionalTable in table.AdditionalTables)
                {
                    if (additionalTable.Finalizers == null)
                        throw new ProcessParameterNullException(this, nameof(ResilientTable.Finalizers));
                }
            }
        }

        try
        {
            CreateTempTables();

            if (FlowState.IsTerminating)
                return;

            InitializeScope();
            if (FlowState.Failed)
            {
                Context.Log(LogSeverity.Information, this, "initialization failed after {Elapsed}", InvocationInfo.InvocationStarted.Elapsed);
                return;
            }

            foreach (var table in Tables)
            {
                if (FlowState.Failed)
                    return;

                for (var partitionIndex = 0; ; partitionIndex++)
                {
                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    if (table.JobCreator != null)
                    {
                        Context.Log(LogSeverity.Information, this, "processing table {TableName}",
                            ConnectionString.Unescape(table.TableName));

                        IProcess[] mainProcessList;

                        using (var creatorScope = Context.BeginTransactionScope(this, creatorScopeKind, LogSeverity.Information))
                        {
                            mainProcessList = table.JobCreator
                                .Invoke(table)
                                .Where(x => x != null)
                                .ToArray();
                        }

                        foreach (var process in mainProcessList)
                        {
                            process.Execute(this);
                            if (FlowState.IsTerminating)
                                return;
                        }

                        break;
                    }

                    Context.Log(LogSeverity.Information, this, "processing table {TableName}, (partition #{PartitionIndex})",
                        ConnectionString.Unescape(table.TableName), partitionIndex);

                    ISequence mainProducer;

                    using (var creatorScope = Context.BeginTransactionScope(this, creatorScopeKind, LogSeverity.Information))
                    {
                        mainProducer = table.PartitionedProducerCreator.Invoke(table, partitionIndex);
                    }

                    var rowCount = mainProducer.CountRowsAndReleaseOwnership(null);

                    if (FlowState.IsTerminating)
                        return;

                    if (rowCount == 0)
                        break;
                }
            }

            FinalizeScope();
        }
        finally
        {
            if (TempTableMode != ResilientSqlScopeTempTableMode.AlwaysKeep)
            {
                if (!FlowState.Failed || TempTableMode == ResilientSqlScopeTempTableMode.AlwaysDrop)
                {
                    DropTempTables();
                }
            }
        }
    }

    private int CountTempRecordsIn(ResilientTableBase table)
    {
        var count = new GetTableRecordCount(Context)
        {
            Name = "TempRecordCountReader",
            ConnectionString = ConnectionString,
            TableName = table.TempTableName,
            WhereClause = null,
        }.ExecuteWithResult(this);

        return count;
    }

    private void CreateTempTables()
    {
        var config = new List<TableCopyConfiguration>();
        foreach (var table in Tables)
        {
            config.Add(new TableCopyConfiguration()
            {
                SourceTableName = table.TableName,
                TargetTableName = table.TempTableName,
                Columns = table.Columns?.ToDictionary(x => x),
            });

            if (table.AdditionalTables != null)
            {
                foreach (var additionalTable in table.AdditionalTables)
                {
                    config.Add(new TableCopyConfiguration()
                    {
                        SourceTableName = additionalTable.TableName,
                        TargetTableName = additionalTable.TempTableName,
                        Columns = additionalTable.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
                    });
                }
            }
        }

        new CopyTableStructure(Context)
        {
            Name = "RecreateTempTables",
            ConnectionString = ConnectionString,
            SuppressExistingTransactionScope = true,
            Configuration = config,
        }.Execute(this);
    }

    private void DropTempTables()
    {
        var tempTableNames = Tables
            .Select(x => x.TempTableName);

        var additionalTempTableNames = Tables
            .Where(x => x.AdditionalTables != null)
            .SelectMany(x => x.AdditionalTables.Select(y => y.TempTableName));

        new DropTables(Context)
        {
            Name = "DropTempTables",
            ConnectionString = ConnectionString,
            TableNames = tempTableNames
                .Concat(additionalTempTableNames)
                .ToArray(),
        }.Execute(this);
    }

    private class TableWithOrder
    {
        internal ResilientTable Table { get; set; }
        internal int OriginalIndex { get; set; }
    }
}


[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ResilientSqlScopeFluent
{
    public static IFlow ResilientSqlScope(this IFlow builder, Func<ResilientSqlScope> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}