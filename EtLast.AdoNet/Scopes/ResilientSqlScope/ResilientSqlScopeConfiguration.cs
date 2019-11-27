namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public delegate IEnumerable<IExecutable> ResilientSqlScopeExecutableCreatorDelegate(string connectionStringKey, ResilientSqlScopeConfiguration configuration);

    public enum ResilientSqlScopeTempTableMode
    {
        KeepOnlyOnFailure, AlwaysKeep, AlwaysDrop
    }

    public class ResilientSqlScopeConfiguration
    {
        public ResilientSqlScope Scope { get; internal set; }

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.Required"/>.
        /// </summary>
        public TransactionScopeKind InitializationTransactionScopeKind { get; set; }

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.Required"/>.
        /// </summary>
        public TransactionScopeKind FinalizerTransactionScopeKind { get; set; }

        /// <summary>
        /// The number of retries of finalizers. Default value is 0. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public int FinalizerRetryCount { get; set; }

        /// <summary>
        /// Default value is <see cref="ResilientSqlScopeTempTableMode.KeepOnlyOnFailure"/>.
        /// </summary>
        public ResilientSqlScopeTempTableMode TempTableMode { get; set; } = ResilientSqlScopeTempTableMode.KeepOnlyOnFailure;

        public string ConnectionStringKey { get; set; }

        /// <summary>
        /// Allows the execution of initializers BEFORE the individual table processes are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate InitializerCreator { get; set; }

        /// <summary>
        /// Allows the execution of global finalizers BEFORE the individual table finalizers are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate PreFinalizerCreator { get; set; }

        /// <summary>
        /// Allows the execution of global finalizers AFTER the individual table finalizers are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate PostFinalizerCreator { get; set; }

        public List<ResilientTable> Tables { get; set; }

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePrefix { get; set; }

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePostfix { get; set; }

        public AdditionalData AdditionalData { get; set; }
    }
}