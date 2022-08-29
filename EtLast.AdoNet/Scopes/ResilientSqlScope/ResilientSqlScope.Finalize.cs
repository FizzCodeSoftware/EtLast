namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private bool Finalize(int initialExceptionCount)
    {
        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (Context.IsTerminating)
                return false;

            // todo: Exception management in IEtlContext is not thread safe

            Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", round);
            try
            {
                using (var scope = Context.BeginScope(this, FinalizerTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecutePreFinalizers();

                    if (Context.ExceptionCount == initialExceptionCount)
                    {
                        CreateAndExecuteFinalizers();
                        if (Context.ExceptionCount == initialExceptionCount)
                        {
                            CreateAndExecutePostFinalizers();

                            if (Context.ExceptionCount == initialExceptionCount)
                                scope.Complete();
                        }
                    }
                } // dispose scope
            }
            catch (Exception ex)
            {
                AddException(ex);
            }

            if (Context.ExceptionCount == initialExceptionCount)
                return true;

            if (round < FinalizerRetryCount)
            {
                Context.ResetInternalCancellationToken();
                Context.ResetExceptionCount(initialExceptionCount);
            }
        }

        return false;
    }

    private void CreateAndExecutePostFinalizers()
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
                    var preExceptionCount = Context.ExceptionCount;

                    process.Execute(this);

                    if (Context.ExceptionCount > preExceptionCount)
                        break;
                }
            }
        }
    }

    private void CreateAndExecuteFinalizers()
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
                    var preExceptionCount = Context.ExceptionCount;

                    process.Execute(this);

                    if (Context.ExceptionCount > preExceptionCount)
                        break;
                }
            }
        }
    }

    private void CreateAndExecutePreFinalizers()
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
                var preExceptionCount = Context.ExceptionCount;

                process.Execute(this);

                if (Context.ExceptionCount > preExceptionCount)
                    break;
            }
        }
    }
}