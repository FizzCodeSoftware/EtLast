namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public delegate List<IJob> DwhBeforeAfterFinalizerJobsCreatorDelegate(string connectionStringKey, DwhStrategyConfiguration configuration);

    public enum DwhStrategyTempTableMode
    {
        KeepOnlyOnFailure, AlwaysKeep, AlwaysDrop
    }

    public class DwhStrategyConfiguration
    {
        public DwhStrategy Strategy { get; internal set; }

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.Required"/>.
        /// </summary>
        public TransactionScopeKind FinalizerTransactionScopeKind { get; set; }

        /// <summary>
        /// The number of retries of finalizers. Default value is 0. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public int FinalizerRetryCount { get; set; }

        /// <summary>
        /// Default value is <see cref="DwhStrategyTempTableMode.KeepOnlyOnFailure"/>.
        /// </summary>
        public DwhStrategyTempTableMode TempTableMode { get; set; } = DwhStrategyTempTableMode.KeepOnlyOnFailure;

        public string ConnectionStringKey { get; set; }

        /// <summary>
        /// Allows the execution of jobs BEFORE the individual table finalizers are created and executed.
        /// </summary>
        public DwhBeforeAfterFinalizerJobsCreatorDelegate BeforeFinalizersJobCreator { get; set; }

        /// <summary>
        /// Allows the execution of jobs AFTER the individual table finalizers are created and executed.
        /// </summary>
        public DwhBeforeAfterFinalizerJobsCreatorDelegate AfterFinalizersJobCreator { get; set; }

        public List<DwhStrategyTableConfiguration> Tables { get; set; }

        /// <summary>
        /// Used for table configurations where <see cref="DwhStrategyTableConfigurationBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePrefix { get; set; }

        /// <summary>
        /// Used for table configurations where <see cref="DwhStrategyTableConfigurationBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePostfix { get; set; }

        public AdditionalData AdditionalData { get; set; }
    }
}