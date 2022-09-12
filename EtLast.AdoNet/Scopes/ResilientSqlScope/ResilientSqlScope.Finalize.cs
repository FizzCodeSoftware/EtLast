namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private void Finalize(ProcessInvocationContext invocationContext)
    {
        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (InvocationContext.IsTerminating)
                return;

            Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", round);
            try
            {
                using (var scope = Context.BeginScope(this, FinalizerTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecutePreFinalizers(invocationContext);

                    if (!invocationContext.IsTerminating)
                    {
                        CreateAndExecuteFinalizers(invocationContext);
                        if (!invocationContext.IsTerminating)
                        {
                            CreateAndExecutePostFinalizers(invocationContext);

                            if (!invocationContext.IsTerminating)
                                scope.Complete();
                        }
                    }
                } // dispose scope
            }
            catch (Exception ex)
            {
                invocationContext.AddException(this, ex);
            }

            if (!invocationContext.IsTerminating)
                return;

            if (round >= FinalizerRetryCount)
            {
                InvocationContext.TakeExceptions(invocationContext);
            }
        }
    }

    private void CreateAndExecutePostFinalizers(ProcessInvocationContext invocationContext)
    {
        if (PostFinalizers != null)
        {
            IJob[] processes;

            using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
            {
                var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
                PostFinalizers.Invoke(builder);
                processes = builder.Jobs.Where(x => x != null).ToArray();
            }

            if (processes?.Length > 0)
            {
                Context.Log(LogSeverity.Debug, this, "created {PostFinalizerCount} post-finalizer(s)",
                    processes?.Length ?? 0);

                foreach (var process in processes)
                {
                    process.Execute(this, invocationContext);

                    if (invocationContext.IsTerminating)
                        break;
                }
            }
        }
    }

    private void CreateAndExecuteFinalizers(ProcessInvocationContext invocationContext)
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

            var allFinalizers = new Dictionary<string, IJob[]>();
            using (var creatorScope = Context.BeginScope(this, creatorScopeKind, LogSeverity.Information))
            {
                var builder = new ResilientSqlTableTableFinalizerBuilder() { Table = table };
                table.Finalizers.Invoke(builder);
                var finalizers = builder.Finalizers.Where(x => x != null).ToArray();

                allFinalizers[table.TableName] = finalizers;

                Context.Log(LogSeverity.Debug, this, "created {FinalizerCount} finalizer(s) for {TableName} (record count: {RecordCount})",
                    finalizers.Length,
                    ConnectionString.Unescape(table.TableName),
                    recordCounts[i]);

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
                foreach (var process in tableFinalizers.Value)
                {
                    process.Execute(this, invocationContext);

                    if (invocationContext.IsTerminating)
                        break;
                }
            }
        }
    }

    private void CreateAndExecutePreFinalizers(ProcessInvocationContext invocationContext)
    {
        if (PreFinalizers == null)
            return;

        IJob[] processes;

        using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
        {
            var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
            PreFinalizers.Invoke(builder);
            processes = builder.Jobs.Where(x => x != null).ToArray();
        }

        if (processes?.Length > 0)
        {
            Context.Log(LogSeverity.Debug, this, "created {PreFinalizerCount} pre-finalizer(s)",
                processes?.Length ?? 0);

            foreach (var process in processes)
            {
                process.Execute(this, invocationContext);

                if (invocationContext.IsTerminating)
                    break;
            }
        }
    }
}