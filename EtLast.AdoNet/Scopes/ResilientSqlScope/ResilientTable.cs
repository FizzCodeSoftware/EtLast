namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate IEvaluable ResilientTablePartitionedMainProcessCreatorDelegate(ResilientTableBase table, int partitionIndex);
    public delegate IEnumerable<IExecutable> ResilientTableMainProcessCreatorDelegate(ResilientTableBase table);
    public delegate IEnumerable<IExecutable> ResilientSqlScopeFinalizerCreatorDelegate(ResilientTableBase table);

    [DebuggerDisplay("{TableName}")]
    public class ResilientTable : ResilientTableBase
    {
        /// <summary>
        /// Setting this to true forces the scope to suppress the ambient transaction scope while calling the process- and finalizer creator delegates. Default value is false.
        /// </summary>
        public bool SuppressTransactionScopeForCreators { get; set; }

        public ResilientTablePartitionedMainProcessCreatorDelegate PartitionedMainProcessCreator { get; set; }
        public ResilientTableMainProcessCreatorDelegate MainProcessCreator { get; set; }

        /// <summary>
        /// Default true. Skips finalizers for main table and all additional tables if the sum record count of the main temp table PLUS in all temp tables is zero.
        /// </summary>
        public bool SkipFinalizersIfNoTempData { get; set; } = true;

        /// <summary>
        /// Default 0.
        /// </summary>
        public int OrderDuringFinalization { get; set; }

        public List<ResilientTableBase> AdditionalTables { get; set; }

        public AdditionalData AdditionalData { get; set; }

        public ResilientTableBase GetAdditionalTable(string tableName)
        {
            return AdditionalTables.Find(x => string.Equals(x.TableName, tableName, StringComparison.InvariantCultureIgnoreCase));
        }
    }

    public class ResilientTableBase
    {
        public ResilientSqlScope Scope { get; internal set; }

        public string TableName { get; set; }
        public string TempTableName { get; set; }
        public string[] Columns { get; set; }

        public ResilientSqlScopeFinalizerCreatorDelegate FinalizerCreator { get; set; }

        private ITopic _topic;

        public ITopic Topic
        {
            get => _topic ?? new Topic(Scope.Configuration.ConnectionString.Unescape(TableName), Scope.Topic.Context);
            set => _topic = value;
        }
    }
}