namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;

    public delegate IFinalProcess EtlWrapperWithTempTableCopyFinalizerDelegate(IEtlContext context, string targetTableName);

    public class EtlWrapperWithTempTableCopyFinalizer : IEtlWrapper
    {
        public string ConnectionStringKey { get; }
        public bool DeleteExistingTableContents { get; }
        public string TableName { get; }
        public string TempTableName { get; }
        public string[] Columns { get; }
        public EtlWrapperWithTempTableCopyFinalizerDelegate[] MainProcessCreators { get; }

        public TransactionScopeKind EvaluationTransactionScopeKind { get; }
        public bool SuppressTransactionScopeForCreator { get; }

        public EtlWrapperWithTempTableCopyFinalizer(string connectionStringKey, bool deleteExistingTableContents, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCopyFinalizerDelegate mainProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            ConnectionStringKey = connectionStringKey;
            DeleteExistingTableContents = deleteExistingTableContents;
            TableName = tableName;
            TempTableName = tempTableName;
            Columns = columns;
            MainProcessCreators = new[] { mainProcessCreator };
            EvaluationTransactionScopeKind = evaluationTransactionScopeKind;
            SuppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public EtlWrapperWithTempTableCopyFinalizer(string connectionStringKey, bool deleteExistingTableContents, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCopyFinalizerDelegate[] mainProcessCreators, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            ConnectionStringKey = connectionStringKey;
            DeleteExistingTableContents = deleteExistingTableContents;
            TableName = tableName;
            TempTableName = tempTableName;
            Columns = columns;
            MainProcessCreators = mainProcessCreators;
            EvaluationTransactionScopeKind = evaluationTransactionScopeKind;
            SuppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(IEtlContext context, TimeSpan transactionScopeTimeout)
        {
            var initialExceptionCount = context.GetExceptions().Count;

            using (var scope = EvaluationTransactionScopeKind != TransactionScopeKind.None
                ? new TransactionScope((TransactionScopeOption)EvaluationTransactionScopeKind, transactionScopeTimeout)
                : null)
            {
                JobProcess process = null;
                using (var creatorScope = SuppressTransactionScopeForCreator
                    ? new TransactionScope(TransactionScopeOption.Suppress)
                    : null)
                {
                    process = new JobProcess(context, "TempTableProcess-" + TableName.Replace("[", "").Replace("]", ""));

                    process.AddJob(new CopyTableStructureJob
                    {
                        Name = "CreateTempTable",
                        ConnectionStringKey = ConnectionStringKey,
                        SourceTableName = TableName,
                        TargetTableName = TempTableName,
                        SuppressExistingTransactionScope = true,
                        ColumnMap = Columns?.Select(x => (x, x)).ToList(),
                    });

                    var index = 0;
                    foreach (var creator in MainProcessCreators)
                    {
                        process.AddJob(new EvaluateProcessWithoutResultJob
                        {
                            Name = MainProcessCreators.Length == 1
                                ? "FillTempTable"
                                : "FillTempTable-" + index.ToString("D", CultureInfo.InvariantCulture),
                            Process = creator.Invoke(context, TempTableName),
                        });

                        index++;
                    }

                    if (DeleteExistingTableContents)
                    {
                        process.AddJob(new DeleteTableJob
                        {
                            Name = "DeleteContentFromTargetTable",
                            ConnectionStringKey = ConnectionStringKey,
                            TableName = TableName,
                        });
                    }

                    process.AddJob(new CopyTableIntoExistingTableJob
                    {
                        Name = "Finalizer-CopyTempToTargetTable",
                        ConnectionStringKey = ConnectionStringKey,
                        SourceTableName = TempTableName,
                        TargetTableName = TableName,
                        ColumnMap = Columns?.Select(x => (x, x)).ToList(),
                        CommandTimeout = 60 * 60,
                    });

                    process.AddJob(new DropTableJob
                    {
                        Name = "DropTempTable",
                        ConnectionStringKey = ConnectionStringKey,
                        TableName = TempTableName,
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