namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;

    public enum ResilientSqlScopeTempTableMode
    {
        KeepOnlyOnFailure, AlwaysKeep, AlwaysDrop
    }

    public sealed class ResilientSqlScopeConfiguration
    {
        public ResilientSqlScope Scope { get; internal set; }

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public TransactionScopeKind InitializationTransactionScopeKind { get; init; } = TransactionScopeKind.RequiresNew;

        /// <summary>
        /// The transaction scope kind around the finalizers. Default value is <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public TransactionScopeKind FinalizerTransactionScopeKind { get; init; } = TransactionScopeKind.RequiresNew;

        /// <summary>
        /// The number of retries of finalizers. Default value is 3. Retrying finalizers is only supported if <seealso cref="FinalizerTransactionScopeKind"/> is set to <see cref="TransactionScopeKind.RequiresNew"/>.
        /// </summary>
        public int FinalizerRetryCount { get; init; } = 3;

        /// <summary>
        /// Default value is <see cref="ResilientSqlScopeTempTableMode.KeepOnlyOnFailure"/>.
        /// </summary>
        public ResilientSqlScopeTempTableMode TempTableMode { get; init; } = ResilientSqlScopeTempTableMode.KeepOnlyOnFailure;

        public NamedConnectionString ConnectionString { get; init; }

        /// <summary>
        /// Allows the execution of initializers BEFORE the individual table processes are created and executed.
        /// </summary>
        public Action<ResilientSqlScopeProcessBuilder> Initializers { get; init; }

        /// <summary>
        /// Allows the execution of global finalizers BEFORE the individual table finalizers are created and executed.
        /// </summary>
        public Action<ResilientSqlScopeProcessBuilder> PreFinalizers { get; init; }

        /// <summary>
        /// Allows the execution of global finalizers AFTER the individual table finalizers are created and executed.
        /// </summary>
        public Action<ResilientSqlScopeProcessBuilder> PostFinalizers { get; init; }

        public List<ResilientTable> Tables { get; init; }

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is "__".
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePrefix { get; init; } = "__";

        /// <summary>
        /// Used for table configurations where <see cref="ResilientTableBase.TempTableName"/> is null.
        /// Temp table name will be: AutoTempTablePrefix + TableName + AutoTempTablePostfix
        /// </summary>
        public string AutoTempTablePostfix { get; init; }

        public AdditionalData AdditionalData { get; init; }
    }
}