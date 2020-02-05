namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using FizzCode.EtLast.AdoNet;

    public class DwhConfiguration
    {
        public string ValidFromColumnName { get; set; }
        public string ValidToColumnName { get; set; }

        public string HistoryTableNamePostfix { get; set; }
        public string HistoryTableIdColumnPostfix { get; set; } = "ID";

        public string TempTableNamePrefix { get; set; }
        public int FinalizerRetryCount { get; set; } = 5;
        public ResilientSqlScopeTempTableMode TempTableMode { get; set; } = ResilientSqlScopeTempTableMode.AlwaysKeep;

        /// <summary>
        /// Default false.
        /// </summary>
        public bool UseContextCreationTimeForNewRecords { get; set; }

        public DateTimeOffset? InfinitePastDateTime { get; set; }
        public DateTimeOffset? InfiniteFutureDateTime { get; set; }
        public bool IncrementalLoadEnabled { get; set; } = true;

        public string EtlRunTableName { get; set; } = "_EtlRun";
        public string EtlInsertRunIdColumnName { get; set; } = "EtlInsertRunId";
        public string EtlUpdateRunIdColumnName { get; set; } = "EtlUpdateRunId";
    }
}