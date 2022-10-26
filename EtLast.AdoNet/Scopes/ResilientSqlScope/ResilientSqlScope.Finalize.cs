﻿namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private void Finalize(Pipe pipe)
    {
        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (Pipe.IsTerminating)
                return;

            Context.Log(LogSeverity.Information, this, "finalization round {FinalizationRound} started", round);
            try
            {
                using (var scope = Context.BeginTransactionScope(this, FinalizerTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecutePreFinalizers(pipe);

                    if (!pipe.IsTerminating)
                    {
                        CreateAndExecuteFinalizers(pipe);
                        if (!pipe.IsTerminating)
                        {
                            CreateAndExecutePostFinalizers(pipe);

                            if (!pipe.IsTerminating)
                                scope.Complete();
                        }
                    }
                } // dispose scope
            }
            catch (Exception ex)
            {
                pipe.AddException(this, ex);
            }

            if (!pipe.IsTerminating)
                return;

            if (round >= FinalizerRetryCount)
            {
                Pipe.TakeExceptions(pipe);
            }
        }
    }

    private void CreateAndExecutePostFinalizers(Pipe pipe)
    {
        if (PostFinalizers != null)
        {
            IProcess[] processes;

            using (var creatorScope = Context.BeginTransactionScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
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
                    process.Execute(this, pipe);

                    if (pipe.IsTerminating)
                        break;
                }
            }
        }
    }

    private void CreateAndExecuteFinalizers(Pipe pipe)
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

            var allFinalizers = new Dictionary<string, IProcess[]>();
            using (var creatorScope = Context.BeginTransactionScope(this, creatorScopeKind, LogSeverity.Information))
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
                    Context.RegisterScopeAction(new ScopeAction()
                    {
                        Context = Context,
                        Caller = InvocationInfo.Caller,
                        Scope = this,
                        Topic = tableFinalizers.Key,
                        Action = "finalized with " + process.Name + " (" + process.GetType().GetFriendlyTypeName() + ")",
                    });

                    process.Execute(this, pipe);

                    if (pipe.IsTerminating)
                        break;
                }
            }
        }
    }

    private void CreateAndExecutePreFinalizers(Pipe pipe)
    {
        if (PreFinalizers == null)
            return;

        IProcess[] processes;

        using (var creatorScope = Context.BeginTransactionScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
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
                process.Execute(this, pipe);

                if (pipe.IsTerminating)
                    break;
            }
        }
    }
}