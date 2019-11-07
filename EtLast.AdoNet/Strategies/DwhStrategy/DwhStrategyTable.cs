namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public delegate IEvaluable DwhPartitionedMainProcessCreatorDelegate(DwhStrategyTable table, int partitionIndex);
    public delegate IExecutable DwhMainProcessCreatorDelegate(DwhStrategyTable table);
    public delegate List<IExecutable> DwhFinalizerJobsCreatorDelegate(DwhStrategyTable table);

    public class DwhStrategyTable : DwhStrategyTableBase
    {
        /// <summary>
        /// Setting this to true forces the strategy to suppress the ambient scope while calling the process- and job creator delegates. Default value is false.
        /// </summary>
        public bool SuppressTransactionScopeForCreators { get; set; }

        public DwhPartitionedMainProcessCreatorDelegate PartitionedMainProcessCreator { get; set; }
        public DwhMainProcessCreatorDelegate MainProcessCreator { get; set; }
        public DwhFinalizerJobsCreatorDelegate FinalizerJobsCreator { get; set; }

        public Dictionary<string, DwhStrategyTableBase> AdditionalTables { get; set; }

        public AdditionalData AdditionalData { get; set; }
    }

    public class DwhStrategyTableBase
    {
        public DwhStrategy Strategy { get; internal set; }

        public string TableName { get; set; }
        public string TempTableName { get; set; }
        public string[] Columns { get; set; }
    }
}