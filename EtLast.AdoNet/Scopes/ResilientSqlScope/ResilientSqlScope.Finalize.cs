namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractExecutable, IScope
{
    private bool Finalize(ref int initialExceptionCount)
    {
        for (var retryCounter = 0; retryCounter <= FinalizerRetryCount; retryCounter++)
        {
            Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", retryCounter);
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

            initialExceptionCount = Context.ExceptionCount;
        }

        return false;
    }

    private void CreateAndExecutePostFinalizers()
    {
        if (PostFinalizers != null)
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
    }

    private void CreateAndExecutePreFinalizers()
    {
        if (PreFinalizers == null)
            return;

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
}