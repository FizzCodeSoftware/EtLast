namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;

    public delegate Tuple<IFinalProcess, List<IJob>> EtlWrapperWithTempTableCustomFinalizerDelegate(IEtlContext context, string tableName, string tempTableName);

    /// <summary>
    /// The ADO.Net implementation of the <see cref="IEtlWrapper"/> interface, optionally supporting transaction scopes.
    /// A temp database table will be created by the wrapper, and filled by the main process.
    /// Then the finalizer jobs will be executed, and at the end the temp table will be dropped by the wrapper.
    /// Usually used in conjunction with <see cref="MsSqlWriteToTableWithMicroTransactionsOperation"/> to fill the temp tables because that operation creates a separate transaction scope for each write batch.
    /// </summary>
    public class EtlWrapperWithTempTableCustomFinalizer : IEtlWrapper
    {
        private readonly string _connectionStringKey;
        private readonly string _tableName;
        private readonly string _tempTableName;
        private readonly string[] _columns;
        private readonly EtlWrapperWithTempTableCustomFinalizerDelegate _mainProcessCreator;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="EtlWrapperWithTempTableCustomFinalizer"/> using a <paramref name="mainProcessCreator"/> delegate which takes an <see cref="IEtlContext"/> and the target table name (which is the <paramref name="tempTableName"/>) and returns a tuple with single new <see cref="IFinalProcess"/> and a list of finalizer jobs to be executed by the wrapper.
        /// </summary>
        /// <param name="connectionStringKey">The connection string key used by the database operations.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="tempTableName">The name of the temp table. This value will be passed to the <paramref name="mainProcessCreator"/> so the main process will write the records directly to the temp table.</param>
        /// <param name="columns">The columns to be used in the temp table from the target table when it is created.</param>
        /// <param name="mainProcessCreator">The delegate which returns the main process and the list of finalizer jobs.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the <paramref name="processCreator"/> delegate.</param>
        public EtlWrapperWithTempTableCustomFinalizer(string connectionStringKey, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCustomFinalizerDelegate mainProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _connectionStringKey = connectionStringKey;
            _tableName = tableName;
            _tempTableName = tempTableName;
            _columns = columns;
            _mainProcessCreator = mainProcessCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(IEtlContext context, TimeSpan transactionScopeTimeout)
        {
            var initialExceptionCount = context.GetExceptions().Count;

            using (var scope = _evaluationTransactionScopeKind != TransactionScopeKind.None
                ? new TransactionScope((TransactionScopeOption)_evaluationTransactionScopeKind, transactionScopeTimeout)
                : null)
            {
                JobProcess process = null;
                using (var creatorScope = _suppressTransactionScopeForCreator
                    ? new TransactionScope(TransactionScopeOption.Suppress)
                    : null)
                {
                    process = new JobProcess(context, "TempTableWrapper-" + _tableName.Replace("[", "").Replace("]", ""));

                    process.AddJob(new CopyTableStructureJob
                    {
                        Name = "CreateTempTable",
                        ConnectionStringKey = _connectionStringKey,
                        SourceTableName = _tableName,
                        TargetTableName = _tempTableName,
                        SuppressExistingTransactionScope = true,
                        ColumnConfiguration = _columns?.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                    });

                    var created = _mainProcessCreator.Invoke(context, _tableName, _tempTableName);

                    process.AddJob(new EvaluateProcessWithoutResultJob()
                    {
                        Name = "FillTempTable",
                        Process = created.Item1,
                    });

                    var index = 0;
                    foreach (var job in created.Item2)
                    {
                        job.Name = created.Item2.Count == 1
                            ? "Finalizer-"
                            : "Finalizer-" + index.ToString("D", CultureInfo.InvariantCulture);
                        process.AddJob(job);

                        index++;
                    }

                    process.AddJob(new DropTableJob
                    {
                        Name = "DropTempTable",
                        ConnectionStringKey = _connectionStringKey,
                        TableName = _tempTableName,
                        SuppressExistingTransactionScope = false,
                    });
                }

                process.EvaluateWithoutResult();

                if (scope != null && context.GetExceptions().Count == initialExceptionCount)
                {
                    scope.Complete();
                }
            }
        }
    }
}