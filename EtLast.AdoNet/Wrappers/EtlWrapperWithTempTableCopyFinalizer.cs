namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;

    public delegate IFinalProcess EtlWrapperWithTempTableCopyFinalizerDelegate(IEtlContext context, string tempTableName);

    /// <summary>
    /// The ADO.Net implementation of the <see cref="IEtlWrapper"/> interface, optionally supporting transaction scopes.
    /// A temp database table will be created by the wrapper, filled by the process(es) created by the supplied delegates.
    /// The contents of the temp table will be copied to the target table and it will be dropped by the wrapper at the end.
    /// Usually used in conjunction with <see cref="MsSqlWriteToTableWithMicroTransactionsOperation"/> to fill the temp tables because that operation creates a separate transaction scope for each write batch.
    /// </summary>
    public class EtlWrapperWithTempTableCopyFinalizer : IEtlWrapper
    {
        private readonly string _connectionStringKey;
        private readonly bool _deleteExistingTableContents;
        private readonly string _tableName;
        private readonly string _tempTableName;
        private readonly string[] _columns;
        private readonly EtlWrapperWithTempTableCopyFinalizerDelegate[] _mainProcessCreators;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="EtlWrapperWithTempTableCopyFinalizer"/> using a process creator delegate which takes an <see cref="IEtlContext"/> and the target table name (which is the <paramref name="tempTableName"/>) and returns a single new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// </summary>
        /// <param name="connectionStringKey">The connection string key used by the database operations.</param>
        /// <param name="deleteExistingTableContents">If set to true, then the contents of the <paramref name="tableName"/> will be deleted by the wrapper be before it copy the temp table to the target table.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="tempTableName">The name of the temp table. This value will be passed to the <paramref name="mainProcessCreator"/> so the main process will write the records directly to the temp table.</param>
        /// <param name="columns">The columns to be copied from the temp table to the target table. If null then all columns will be copied. Also this column list specifies the list of columns used in the temp table from the target table when it is created.</param>
        /// <param name="mainProcessCreator">The delegate which returns the process.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public EtlWrapperWithTempTableCopyFinalizer(string connectionStringKey, bool deleteExistingTableContents, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCopyFinalizerDelegate mainProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _connectionStringKey = connectionStringKey;
            _deleteExistingTableContents = deleteExistingTableContents;
            _tableName = tableName;
            _tempTableName = tempTableName;
            _columns = columns;
            _mainProcessCreators = new[] { mainProcessCreator };
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="EtlWrapperWithTempTableCopyFinalizer"/> using one or more process creator delegates whose take an <see cref="IEtlContext"/> and the target table name (which is the <paramref name="tempTableName"/>) and returns a single new <see cref="IFinalProcess"/> each to be executed by the wrapper.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="connectionStringKey">The connection string key used by the database operations.</param>
        /// <param name="deleteExistingTableContents">If set to true, then the contents of the <paramref name="tableName"/> will be deleted by the wrapper be before it copy the temp table to the target table.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="tempTableName">The name of the temp table. This value will be passed to the main process creators so the main process will write the records directly to the temp table.</param>
        /// <param name="columns">The columns to be copied from the temp table to the target table. If null then all columns will be copied. Also this column list specifies the list of columns used in the temp table from the target table when it is created.</param>
        /// <param name="mainProcessCreators">The delegates whose return one process each.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public EtlWrapperWithTempTableCopyFinalizer(string connectionStringKey, bool deleteExistingTableContents, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCopyFinalizerDelegate[] mainProcessCreators, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _connectionStringKey = connectionStringKey;
            _deleteExistingTableContents = deleteExistingTableContents;
            _tableName = tableName;
            _tempTableName = tempTableName;
            _columns = columns;
            _mainProcessCreators = mainProcessCreators;
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

                    var index = 0;
                    foreach (var creator in _mainProcessCreators)
                    {
                        process.AddJob(new EvaluateProcessWithoutResultJob
                        {
                            Name = _mainProcessCreators.Length == 1
                                ? "FillTempTable"
                                : "FillTempTable-" + index.ToString("D", CultureInfo.InvariantCulture),
                            Process = creator.Invoke(context, _tempTableName),
                        });

                        index++;
                    }

                    if (_deleteExistingTableContents)
                    {
                        process.AddJob(new DeleteTableJob
                        {
                            Name = "DeleteContentFromTargetTable",
                            ConnectionStringKey = _connectionStringKey,
                            TableName = _tableName,
                        });
                    }

                    process.AddJob(new CopyTableIntoExistingTableJob
                    {
                        Name = "Finalizer-CopyTempToTargetTable",
                        ConnectionStringKey = _connectionStringKey,
                        SourceTableName = _tempTableName,
                        TargetTableName = _tableName,
                        ColumnConfiguration = _columns?.Select(x => new ColumnCopyConfiguration(x)).ToList(),
                        CommandTimeout = 60 * 60,
                    });

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