namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public delegate IEvaluable ResilientTablePartitionedMainProcessCreatorDelegate(ResilientTable table, int partitionIndex);
    public delegate IEnumerable<IExecutable> ResilientTableMainProcessCreatorDelegate(ResilientTable table);
    public delegate IEnumerable<IExecutable> ResilientSqlScopeFinalizerCreatorDelegate(ResilientTable table);

    public class ResilientTable : ResilientTableBase
    {
        /// <summary>
        /// Setting this to true forces the scope to suppress the ambient transaction scope while calling the process- and finalizer creator delegates. Default value is false.
        /// </summary>
        public bool SuppressTransactionScopeForCreators { get; set; }

        public ResilientTablePartitionedMainProcessCreatorDelegate PartitionedMainProcessCreator { get; set; }
        public ResilientTableMainProcessCreatorDelegate MainProcessCreator { get; set; }
        public ResilientSqlScopeFinalizerCreatorDelegate FinalizerCreator { get; set; }

        public Dictionary<string, ResilientTableBase> AdditionalTables { get; set; }

        public AdditionalData AdditionalData { get; set; }
    }

    public class ResilientTableBase
    {
        public ResilientSqlScope Scope { get; internal set; }

        public string TableName { get; set; }
        public string TempTableName { get; set; }
        public string[] Columns { get; set; }

        private string _topic;

        public string Topic
        {
            get => _topic ?? Scope.Configuration.ConnectionString.Unescape(TableName);
            set => _topic = value;
        }
    }
}