namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public delegate IFinalProcess DwhMainProcessCreatorDelegate(string connectionStringKey, DwhStrategyTableConfiguration tableConfiguration, int partitionIndex, out bool processSupportsPartitions);
    public delegate List<IJob> DwhFinalizerJobsCreatorDelegate(string connectionStringKey, DwhStrategyTableConfiguration tableConfiguration);

    public class DwhStrategyTableConfiguration : DwhStrategyTableConfigurationBase
    {
        /// <summary>
        /// Setting this to true forces the strategy to suppress the ambient scope while calling the process- and job creator delegates. Default value is false.
        /// </summary>
        public bool SuppressTransactionScopeForCreators { get; set; }

        public DwhMainProcessCreatorDelegate MainProcessCreator { get; set; }
        public DwhFinalizerJobsCreatorDelegate FinalizerJobsCreator { get; set; }

        public Dictionary<string, DwhStrategyTableConfigurationBase> AdditionalTables { get; set; }
    }

    public class DwhStrategyTableConfigurationBase
    {
        public string TableName { get; set; }
        public string TempTableName { get; set; }
        public string[] Columns { get; set; }
    }
}