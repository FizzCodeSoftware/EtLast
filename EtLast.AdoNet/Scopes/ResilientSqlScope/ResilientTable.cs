namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate IEvaluable ResilientTablePartitionedMainProcessCreatorDelegate(ResilientTable table, int partitionIndex);
    public delegate IEnumerable<IExecutable> ResilientTableMainProcessCreatorDelegate(ResilientTable table);
    public delegate IEnumerable<IExecutable> ResilientSqlScopeFinalizerCreatorDelegate(ResilientTable table);

    [DebuggerDisplay("{TableName}")]
    public class ResilientTable : ResilientTableBase
    {
        /// <summary>
        /// Setting this to true forces the scope to suppress the ambient transaction scope while calling the process- and finalizer creator delegates. Default value is false.
        /// </summary>
        public bool SuppressTransactionScopeForCreators { get; set; }

        public ResilientTablePartitionedMainProcessCreatorDelegate PartitionedMainProcessCreator { get; set; }
        public ResilientTableMainProcessCreatorDelegate MainProcessCreator { get; set; }

        public ResilientSqlScopeFinalizerCreatorDelegate FinalizerCreator { get; set; }

        /// <summary>
        /// Default 0.
        /// </summary>
        public int OrderDuringFinalization { get; set; }

        public Dictionary<string, ResilientTableBase> AdditionalTables { get; set; }

        public AdditionalData AdditionalData { get; set; }
    }

    public class ResilientTableBase
    {
        public ResilientSqlScope Scope { get; internal set; }

        public string TableName { get; set; }
        public string TempTableName { get; set; }
        public string[] Columns { get; set; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool SkipFinalizersIfTempTableIsEmpty { get; set; } = true;

        private ITopic _topic;

        public ITopic Topic
        {
            get => _topic ?? new Topic(Scope.Configuration.ConnectionString.Unescape(TableName), Scope.Topic.Context);
            set => _topic = value;
        }
    }
}