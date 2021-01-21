namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;

    public delegate IEnumerable<IExecutable> ResilientSqlScopeExecutableCreatorDelegate(ResilientSqlScope scope, IProcess caller);

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
        public TransactionScopeKind InitializationTransactionScopeKind { get; init; }

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.Required"/>.
        /// </summary>
        public TransactionScopeKind FinalizerTransactionScopeKind { get; init; }

        /// <summary>
        /// The number of retries of finalizers. Default value is 0. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public int FinalizerRetryCount { get; init; }

        /// <summary>
        /// Default value is <see cref="ResilientSqlScopeTempTableMode.KeepOnlyOnFailure"/>.
        /// </summary>
        public ResilientSqlScopeTempTableMode TempTableMode { get; init; } = ResilientSqlScopeTempTableMode.KeepOnlyOnFailure;

        public NamedConnectionString ConnectionString { get; init; }

        /// <summary>
        /// Allows the execution of initializers BEFORE the individual table processes are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate InitializerCreator { get; init; }

        /// <summary>
        /// Allows the execution of global finalizers BEFORE the individual table finalizers are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate PreFinalizerCreator { get; init; }

        /// <summary>
        /// Allows the execution of global finalizers AFTER the individual table finalizers are created and executed.
        /// </summary>
        public ResilientSqlScopeExecutableCreatorDelegate PostFinalizerCreator { get; init; }

        public List<ResilientTable> Tables { get; init; }

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePrefix { get; init; }

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePostfix { get; init; }

        public AdditionalData AdditionalData { get; init; }
    }
}