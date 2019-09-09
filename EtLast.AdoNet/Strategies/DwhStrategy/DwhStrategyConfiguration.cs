namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public class DwhStrategyConfiguration
    {
        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.Required"/>.
        /// </summary>
        public TransactionScopeKind FinalizerTransactionScopeKind { get; set; }

        /// <summary>
        /// The number of retries of finalizers. Default value is 0. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public int FinalizerRetryCount { get; set; }

        /// <summary>
        /// Setting this to true forces the strategy to clean up the temp tables even if an error caused a failure. Default value is false.
        /// </summary>
        public bool AlwaysDropTempTables { get; set; }

        public string ConnectionStringKey { get; set; }

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
    }
}