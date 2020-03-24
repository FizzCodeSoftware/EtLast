namespace FizzCode.EtLast.DwhBuilder
{
    using System;
    using FizzCode.EtLast.AdoNet;

    public class DwhBuilderConfiguration
    {
        /// <summary>
        /// Default "_ValidFrom"
        /// </summary>
        public string ValidFromColumnName { get; set; } = "_ValidFrom";

        /// <summary>
        /// Default "_ValidTo"
        /// </summary>
        public string ValidToColumnName { get; set; } = "_ValidTo";

        /// <summary>
        /// Default "_hist"
        /// </summary>
        public string HistoryTableNamePostfix { get; set; } = "_hist";

        /// <summary>
        /// Default null which means then the table's name will be used.
        /// </summary>
        public string HistoryTableIdentityColumnBase { get; set; }

        /// <summary>
        /// Default "ID"
        /// </summary>
        public string HistoryTableIdentityColumnPostfix { get; set; } = "ID";

        /// <summary>
        /// Default "_temp_"
        /// </summary>
        public string TempTableNamePrefix { get; set; } = "_temp_";

        /// <summary>
        /// Default 5
        /// </summary>
        public int FinalizerRetryCount { get; set; } = 5;

        /// <summary>
        /// Default <see cref="ResilientSqlScopeTempTableMode.AlwaysKeep"></see>
        /// </summary>
        public ResilientSqlScopeTempTableMode TempTableMode { get; set; } = ResilientSqlScopeTempTableMode.AlwaysKeep;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool UseContextCreationTimeForNewRecords { get; set; }

        /// <summary>
        /// Default new DateTimeOffset(1900, 1, 1, 0, 0, 0, new TimeSpan(0))
        /// </summary>
        public DateTimeOffset? InfinitePastDateTime { get; set; } = new DateTimeOffset(1900, 1, 1, 0, 0, 0, new TimeSpan(0));

        /// <summary>
        /// Default new DateTimeOffset(2500, 1, 1, 0, 0, 0, new TimeSpan(0))
        /// </summary>
        public DateTimeOffset? InfiniteFutureDateTime { get; set; }

        /// <summary>
        /// Default true
        /// </summary>
        public bool IncrementalLoadEnabled { get; set; } = true;

        /// <summary>
        /// Default "_EtlRun"
        /// </summary>
        public string EtlRunTableName { get; set; } = "_EtlRun";

        /// <summary>
        /// Default "EtlInsertRunId"
        /// </summary>
        public string EtlInsertRunIdColumnName { get; set; } = "EtlInsertRunId";

        /// <summary>
        /// Default "EtlUpdateRunId"
        /// </summary>
        public string EtlUpdateRunIdColumnName { get; set; } = "EtlUpdateRunId";
    }
}