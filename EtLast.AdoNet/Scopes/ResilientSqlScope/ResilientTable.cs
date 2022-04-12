namespace FizzCode.EtLast;

public delegate IProducer ResilientTablePartitionedMainProcessCreatorDelegate(ResilientTable table, int partitionIndex);
public delegate IEnumerable<IExecutable> ResilientTableMainProcessCreatorDelegate(ResilientTable table);
public delegate IEnumerable<IExecutable> ResilientSqlScopeFinalizerCreatorDelegate(ResilientTableBase table);

[DebuggerDisplay("{TableName}")]
public sealed class ResilientTable : ResilientTableBase
{
    /// <summary>
    /// Setting this to true forces the scope to suppress the ambient transaction scope while calling the process- and finalizer creator delegates. Default value is false.
    /// </summary>
    public bool SuppressTransactionScopeForCreators { get; set; }

    public ResilientTablePartitionedMainProcessCreatorDelegate PartitionedMainProcessCreator { get; set; }
    public ResilientTableMainProcessCreatorDelegate MainProcessCreator { get; set; }

    /// <summary>
    /// Default true. Skips finalizers for main table and all additional tables if the sum record count of the main temp table PLUS in all temp tables is zero.
    /// </summary>
    public bool SkipFinalizersIfNoTempData { get; set; } = true;

    /// <summary>
    /// Default 0.
    /// </summary>
    public int OrderDuringFinalization { get; set; }

    public List<ResilientTableBase> AdditionalTables { get; set; }

    public AdditionalData AdditionalData { get; set; }

    public ResilientTableBase GetAdditionalTable(string tableName)
    {
        return AdditionalTables.Find(x => string.Equals(x.TableName, tableName, StringComparison.InvariantCultureIgnoreCase));
    }
}

public class ResilientTableBase
{
    public ResilientSqlScope Scope { get; internal set; }

    public string TableName { get; init; }

    private string _tempTableName;
    public string TempTableName
    {
        get => _tempTableName
            ?? Scope.ConnectionString.ChangeObjectIdentifier(TableName, Scope.AutoTempTablePrefix + Scope.ConnectionString.Unescape(Scope.ConnectionString.GetObjectIdentifier(TableName)) + Scope.AutoTempTablePostfix);
        init => _tempTableName = value;
    }

    public string[] Columns { get; init; }

    public Action<ResilientSqlTableTableFinalizerBuilder> Finalizers { get; set; }

    public override string ToString()
    {
        return TableName + (Columns != null ? ": " + string.Join(',', Columns) : "");
    }
}