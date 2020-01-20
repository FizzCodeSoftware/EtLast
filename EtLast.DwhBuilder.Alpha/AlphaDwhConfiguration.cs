namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using FizzCode.EtLast.AdoNet;

    public class AlphaDwhConfiguration
    {
        public string LastModifiedColumnName { get; set; }
        public string ValidFromColumnName { get; set; }
        public string ValidToColumnName { get; set; }

        public string HistoryTableNamePostfix { get; set; }
        public string TempTableNamePrefix { get; set; }
        public int FinalizerRetryCount { get; set; } = 5;
        public ResilientSqlScopeTempTableMode TempTableMode { get; set; } = ResilientSqlScopeTempTableMode.AlwaysKeep;
        public DateTime InfiniteFutureDateTime { get; set; }
        public DateTime InfinitePastDateTime { get; set; }
        public bool IncrementalLoadEnabled { get; set; } = true;

        public bool UseEtlRunTable { get; set; } = true;
        public string EtlRunTableName { get; set; } = "_EtlRun";
        public string EtlInsertRunIdColumnName { get; set; } = "EtlInsertRunId";
        public string EtlUpdateRunIdColumnName { get; set; } = "EtlUpdateRunId";
    }
}