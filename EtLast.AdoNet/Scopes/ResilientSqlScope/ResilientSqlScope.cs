namespace FizzCode.EtLast;

public enum ResilientSqlScopeTempTableMode
{
    KeepOnlyOnFailure, AlwaysKeep, AlwaysDrop
}

public sealed class ResilientSqlScope : AbstractExecutable, IScope
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

    public ResilientSqlScope(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Tables == null)
            throw new ProcessParameterNullException(this, nameof(Tables));

        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    protected override void ExecuteImpl()
    {
        var maxRetryCount = FinalizerRetryCount;
        if (FinalizerTransactionScopeKind != TransactionScopeKind.RequiresNew && maxRetryCount > 0)
            throw new InvalidProcessParameterException(this, nameof(FinalizerRetryCount), null, "retrying finalizers can be possible only if the " + nameof(FinalizerTransactionScopeKind) + " is set to " + nameof(TransactionScopeKind.RequiresNew));

        var initialExceptionCount = Context.ExceptionCount;
        var success = false;

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

            if (table.MainProcessCreator == null && table.PartitionedMainProcessCreator == null)
                throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " must be supplied for " + table.TableName);

            if (table.MainProcessCreator != null && table.PartitionedMainProcessCreator != null)
                throw new InvalidProcessParameterException(this, nameof(ResilientTable.MainProcessCreator) + "/" + nameof(ResilientTable.PartitionedMainProcessCreator), null, "only one of " + nameof(ResilientTable.MainProcessCreator) + " or " + nameof(ResilientTable.PartitionedMainProcessCreator) + " can be supplied for " + table.TableName);

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

            if (Context.ExceptionCount > initialExceptionCount)
                return;

            if (Initializers != null)
            {
                var initializationSuccessful = false;
                Initialize(maxRetryCount, ref initialExceptionCount, ref initializationSuccessful);

                if (!initializationSuccessful)
                {
                    Context.Log(LogSeverity.Information, this, "initialization failed after {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
                    return;
                }
            }

            foreach (var table in Tables)
            {
                for (var partitionIndex = 0; ; partitionIndex++)
                {
                    var creatorScopeKind = table.SuppressTransactionScopeForCreators
                        ? TransactionScopeKind.Suppress
                        : TransactionScopeKind.None;

                    if (table.MainProcessCreator != null)
                    {
                        Context.Log(LogSeverity.Information, this, "processing table {TableName}",
                            ConnectionString.Unescape(table.TableName));

                        IExecutable[] mainProcessList;

                        using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                        {
                            mainProcessList = table.MainProcessCreator
                                .Invoke(table)
                                .Where(x => x != null)
                                .ToArray();
                        }

                        foreach (var process in mainProcessList)
                        {
                            var preExceptionCount = Context.ExceptionCount;
                            process.Execute(this);
                            if (Context.ExceptionCount > initialExceptionCount)
                            {
                                return;
                            }
                        }

                        break;
                    }

                    Context.Log(LogSeverity.Information, this, "processing table {TableName}, (partition #{PartitionIndex})",
                        ConnectionString.Unescape(table.TableName), partitionIndex);

                    IProducer mainEvaluableProcess;

                    using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                    {
                        mainEvaluableProcess = table.PartitionedMainProcessCreator.Invoke(table, partitionIndex);
                    }

                    var rowCount = mainEvaluableProcess.Evaluate(this).CountRowsWithoutTransfer();

                    if (Context.ExceptionCount > initialExceptionCount)
                        return;

                    if (rowCount == 0)
                        break;
                }
            }

            for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
            {
                Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", retryCounter);
                using (var scope = Context.BeginScope(this, FinalizerTransactionScopeKind, LogSeverity.Information))
                {
                    if (PreFinalizers != null)
                    {
                        IExecutable[] finalizers;

                        using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                        {
                            var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                            PreFinalizers.Invoke(builder);
                            finalizers = builder.Processes.Where(x => x != null).ToArray();
                        }

                        if (finalizers?.Length > 0)
                        {
                            Context.Log(LogSeverity.Debug, this, "created {PreFinalizerCount} pre-finalizer(s)",
                                finalizers?.Length ?? 0);

                            foreach (var finalizer in finalizers)
                            {
                                var preExceptionCount = Context.ExceptionCount;

                                Context.Log(LogSeverity.Information, this, "starting pre-finalizer: {Process}",
                                    finalizer.Name);

                                finalizer.Execute(this);
                                if (Context.ExceptionCount > preExceptionCount)
                                    break;
                            }
                        }
                    }

                    if (Context.ExceptionCount == initialExceptionCount)
                    {
                        var tablesOrderedTemp = new List<TableWithOrder>();
                        for (var i = 0; i < Tables.Count; i++)
                        {
                            tablesOrderedTemp.Add(new TableWithOrder()
                            {
                                Table = Tables[i],
                                OriginalIndex = i,
                            });
                        }

                        var tablesOrdered = tablesOrderedTemp
                            .OrderBy(x => x.Table.OrderDuringFinalization)
                            .ThenBy(x => x.OriginalIndex)
                            .Select(x => x.Table)
                            .ToList();

                        var recordCounts = new int[tablesOrdered.Count];
                        for (var i = 0; i < tablesOrdered.Count; i++)
                        {
                            var table = tablesOrdered[i];

                            var recordCount = CountTempRecordsIn(table);
                            if (table.AdditionalTables?.Count > 0)
                            {
                                foreach (var additionalTable in table.AdditionalTables)
                                {
                                    recordCount += CountTempRecordsIn(additionalTable);
                                }
                            }

                            recordCounts[i] = recordCount;
                        }

                        Context.Log(LogSeverity.Information, this, "{TableCountWithData} of {TotalTableCount} temp table contains data",
                            recordCounts.Count(x => x > 0), recordCounts.Length);

                        for (var i = 0; i < tablesOrdered.Count; i++)
                        {
                            Context.Log(LogSeverity.Verbose, this, "temp table {TableName} contains {RecordCount} records",
                                ConnectionString.Unescape(tablesOrdered[i].TempTableName), recordCounts[i]);
                        }

                        for (var i = 0; i < tablesOrdered.Count; i++)
                        {
                            var table = tablesOrdered[i];
                            if (table.SkipFinalizersIfNoTempData && recordCounts[i] == 0)
                            {
                                Context.Log(LogSeverity.Debug, this, "no data found for {TableName}, skipping finalizers",
                                    ConnectionString.Unescape(table.TableName));

                                continue;
                            }

                            var creatorScopeKind = table.SuppressTransactionScopeForCreators
                                ? TransactionScopeKind.Suppress
                                : TransactionScopeKind.None;

                            var allFinalizers = new Dictionary<string, IExecutable[]>();
                            using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
                            {
                                var builder = new ResilientSqlTableTableFinalizerBuilder() { Table = table };
                                table.Finalizers.Invoke(builder);
                                var finalizers = builder.Finalizers.Where(x => x != null).ToArray();

                                allFinalizers[table.TableName] = finalizers;

                                Context.Log(LogSeverity.Debug, this, "created {FinalizerCount} finalizer(s) for {TableName}",
                                    finalizers.Length,
                                    ConnectionString.Unescape(table.TableName));

                                if (table.AdditionalTables != null)
                                {
                                    foreach (var additionalTable in table.AdditionalTables)
                                    {
                                        builder = new ResilientSqlTableTableFinalizerBuilder() { Table = additionalTable };
                                        additionalTable.Finalizers.Invoke(builder);
                                        finalizers = builder.Finalizers.Where(x => x != null).ToArray();

                                        allFinalizers[additionalTable.TableName] = finalizers;

                                        Context.Log(LogSeverity.Debug, this, "created {FinalizerCount} finalizer(s) for {TableName}",
                                            finalizers.Length,
                                            ConnectionString.Unescape(additionalTable.TableName));
                                    }
                                }
                            }

                            foreach (var tableFinalizers in allFinalizers)
                            {
                                foreach (var finalizer in tableFinalizers.Value)
                                {
                                    var preExceptionCount = Context.ExceptionCount;
                                    Context.Log(LogSeverity.Information, this, "finalizing {TableName} with {Process}",
                                        ConnectionString.Unescape(tableFinalizers.Key),
                                        finalizer.Name);

                                    finalizer.Execute(this);
                                    if (Context.ExceptionCount > preExceptionCount)
                                    {
                                        break;
                                    }
                                }
                            }
                        }

                        if (PostFinalizers != null && Context.ExceptionCount == initialExceptionCount)
                        {
                            IExecutable[] finalizers;

                            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                            {
                                var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                                PostFinalizers.Invoke(builder);
                                finalizers = builder.Processes.Where(x => x != null).ToArray();
                            }

                            if (finalizers?.Length > 0)
                            {
                                Context.Log(LogSeverity.Debug, this, "created {PostFinalizerCount} post-finalizer(s)",
                                    finalizers?.Length ?? 0);

                                foreach (var finalizer in finalizers)
                                {
                                    var preExceptionCount = Context.ExceptionCount;

                                    Context.Log(LogSeverity.Information, this, "starting post-finalizer: {Process}",
                                        finalizer.Name);

                                    finalizer.Execute(this);
                                    if (Context.ExceptionCount > preExceptionCount)
                                        break;
                                }
                            }
                        }
                    }

                    var currentExceptionCount = Context.ExceptionCount;
                    if (currentExceptionCount == initialExceptionCount)
                    {
                        scope.Complete();
                    }

                    currentExceptionCount = Context.ExceptionCount;
                    if (currentExceptionCount == initialExceptionCount)
                    {
                        success = true;
                        break;
                    }

                    initialExceptionCount = currentExceptionCount;
                }
            }
        }
        finally
        {
            if (TempTableMode != ResilientSqlScopeTempTableMode.AlwaysKeep)
            {
                if (success || TempTableMode == ResilientSqlScopeTempTableMode.AlwaysDrop)
                {
                    DropTempTables();
                }
            }
        }

        Context.Log(LogSeverity.Information, this, success ? "finished in {Elapsed}" : "failed after {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
    }

    private int CountTempRecordsIn(ResilientTableBase table)
    {
        var count = new GetTableRecordCount(Context)
        {
            Name = "TempRecordCountReader",
            ConnectionString = ConnectionString,
            TableName = table.TempTableName,
        }.Execute(this);

        return count;
    }

    private void Initialize(int maxRetryCount, ref int initialExceptionCount, ref bool initializationSuccessful)
    {
        for (var retryCounter = 0; retryCounter <= maxRetryCount; retryCounter++)
        {
            Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", retryCounter);
            using (var scope = Context.BeginScope(this, InitializationTransactionScopeKind, LogSeverity.Information))
            {
                IExecutable[] initializers;

                using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
                {
                    var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                    Initializers.Invoke(builder);
                    initializers = builder.Processes.Where(x => x != null).ToArray();

                    Context.Log(LogSeverity.Information, this, "created {InitializerCount} initializers", initializers?.Length ?? 0);
                }

                if (initializers?.Length > 0)
                {
                    Context.Log(LogSeverity.Information, this, "starting initializers");

                    foreach (var initializer in initializers)
                    {
                        var preExceptionCount = Context.ExceptionCount;
                        initializer.Execute(this);
                        if (Context.ExceptionCount > preExceptionCount)
                            break;
                    }
                }

                var currentExceptionCount = Context.ExceptionCount;
                if (currentExceptionCount == initialExceptionCount)
                {
                    scope.Complete();

                    initializationSuccessful = true;
                    break;
                }

                initialExceptionCount = currentExceptionCount;
            }
        }
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
                        Columns = table.Columns.ToDictionary(c => c, x => ConnectionString.Escape(x)),
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
