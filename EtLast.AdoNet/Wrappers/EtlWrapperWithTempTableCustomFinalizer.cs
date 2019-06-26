namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;

    public delegate Tuple<IFinalProcess, List<IJob>> EtlWrapperWithTempTableCustomFinalizerDelegate(IEtlContext context, string tableName, string targetTableName);

    public class EtlWrapperWithTempTableCustomFinalizer : IEtlWrapper
    {
        public string ConnectionStringKey { get; }
        public string TableName { get; }
        public string TempTableName { get; }
        public string[] Columns { get; }
        public EtlWrapperWithTempTableCustomFinalizerDelegate MainProcessCreator { get; }

        public TransactionScopeKind EvaluationTransactionScopeKind { get; }
        public bool SuppressTransactionScopeForCreator { get; }

        public EtlWrapperWithTempTableCustomFinalizer(string connectionStringKey, string tableName, string tempTableName, string[] columns, EtlWrapperWithTempTableCustomFinalizerDelegate mainProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            ConnectionStringKey = connectionStringKey;
            TableName = tableName;
            TempTableName = tempTableName;
            Columns = columns;
            MainProcessCreator = mainProcessCreator;
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

                    var created = MainProcessCreator.Invoke(context, TableName, TempTableName);

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