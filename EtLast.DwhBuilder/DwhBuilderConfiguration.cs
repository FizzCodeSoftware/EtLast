namespace FizzCode.EtLast.DwhBuilder;

using System;

public class DwhBuilderConfiguration
{
    /// <summary>
    /// Default true
    /// </summary>
    public bool UseEtlRunInfo { get; set; } = true;

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
    public bool UseEtlRunIdForDefaultValidFrom { get; set; }

    /// <summary>
    /// Default new DateTimeOffset(1900, 1, 1, 0, 0, 0, new TimeSpan(0))
    /// </summary>
    public DateTimeOffset? InfinitePastDateTime { get; set; } = new DateTimeOffset(1900, 1, 1, 0, 0, 0, new TimeSpan(0));

    /// <summary>
    /// Default new DateTimeOffset(2500, 1, 1, 0, 0, 0, new TimeSpan(0))
    /// </summary>
    public DateTimeOffset? InfiniteFutureDateTime { get; set; } = new DateTimeOffset(2500, 1, 1, 0, 0, 0, new TimeSpan(0));

    /// <summary>
    /// Default true
    /// </summary>
    public bool IncrementalLoadEnabled { get; set; } = true;

    /// <summary>
    /// Default "_EtlRun"
    /// </summary>
    public string EtlRunTableName { get; set; } = "_EtlRun";

    /// <summary>
    /// Default "EtlRunInsert"
    /// </summary>
    public string EtlRunInsertColumnName { get; set; } = "EtlRunInsert";

    /// <summary>
    /// Default "EtlUpdateRunId"
    /// </summary>
    public string EtlRunUpdateColumnName { get; set; } = "EtlRunUpdate";

    /// <summary>
    /// Default "EtlRunFrom"
    /// </summary>
    public string EtlRunFromColumnName { get; set; } = "EtlRunFrom";

    /// <summary>
    /// Default "EtlRunTo"
    /// </summary>
    public string EtlRunToColumnName { get; set; } = "EtlRunTo";
}
