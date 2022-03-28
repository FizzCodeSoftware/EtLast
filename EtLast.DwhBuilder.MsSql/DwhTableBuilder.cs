namespace FizzCode.EtLast.DwhBuilder.MsSql;

public delegate IEnumerable<IMutator> MutatorCreatorDelegate(DwhTableBuilder tableBuilder);

[DebuggerDisplay("{Table}")]
public class DwhTableBuilder : IDwhTableBuilder
{
    public MsSqlDwhBuilder DwhBuilder { get; }
    public ResilientTable ResilientTable { get; }
    public RelationalTable Table { get; }

    public bool HasEtlRunInfo { get; }
    public string EtlRunInsertColumnNameEscaped { get; }
    public string EtlRunUpdateColumnNameEscaped { get; }
    public string EtlRunFromColumnNameEscaped { get; }
    public string EtlRunToColumnNameEscaped { get; }

    public RelationalColumn ValidFromColumn { get; }
    public string ValidFromColumnNameEscaped { get; }

    public string ValidToColumnName { get; }
    public string ValidToColumnNameEscaped { get; }

    private readonly List<Func<DwhTableBuilder, IEnumerable<IExecutable>>> _finalizerCreators = new();
    private readonly List<MutatorCreatorDelegate> _mutatorCreators = new();
    private Func<DateTimeOffset?, IProducer> _inputProcessCreator;

    public DwhTableBuilder(MsSqlDwhBuilder builder, ResilientTable resilientTable, RelationalTable table)
    {
        DwhBuilder = builder;
        ResilientTable = resilientTable;
        Table = table;

        HasEtlRunInfo = builder.Configuration.UseEtlRunInfo && !Table.GetEtlRunInfoDisabled();
        if (HasEtlRunInfo)
        {
            EtlRunInsertColumnNameEscaped = Table[builder.Configuration.EtlRunInsertColumnName].NameEscaped(builder.ConnectionString);
            EtlRunUpdateColumnNameEscaped = Table[builder.Configuration.EtlRunUpdateColumnName].NameEscaped(builder.ConnectionString);
            EtlRunFromColumnNameEscaped = Table[builder.Configuration.EtlRunFromColumnName].NameEscaped(builder.ConnectionString);
            EtlRunToColumnNameEscaped = Table[builder.Configuration.EtlRunToColumnName].NameEscaped(builder.ConnectionString);
        }

        ValidFromColumn = Table[builder.Configuration.ValidFromColumnName];
        ValidFromColumnNameEscaped = ValidFromColumn?.NameEscaped(builder.ConnectionString);

        ValidToColumnName = ValidFromColumn != null ? builder.Configuration.ValidToColumnName : null;
        ValidToColumnNameEscaped = ValidToColumnName != null ? builder.ConnectionString.Escape(ValidToColumnName) : null;
    }

    internal void AddMutatorCreator(MutatorCreatorDelegate creator)
    {
        _mutatorCreators.Add(creator);
    }

    internal void AddFinalizerCreator(Func<DwhTableBuilder, IEnumerable<IExecutable>> creator)
    {
        _finalizerCreators.Add(creator);
    }

    internal void SetInputProcessCreator(Func<DateTimeOffset?, IProducer> creator)
    {
        _inputProcessCreator = creator;
    }

    internal void Build()
    {
        ResilientTable.Finalizers = CreateTableFinalizers;
        ResilientTable.MainProcessCreator = _ => CreateTableMainProcess();
    }

    private IMutator CreateTempWriter(ResilientTable table, RelationalTable dwhTable)
    {
        var tempColumns = dwhTable.Columns
            .Where(x => !x.GetUsedByEtlRunInfo());

        if (dwhTable.AnyPrimaryKeyColumnIsIdentity)
        {
            tempColumns = tempColumns
                .Where(x => !x.IsPrimaryKey);
        }

        return new ResilientWriteToMsSqlMutator(table.Scope.Context)
        {
            Name = "TempWriter",
            ConnectionString = table.Scope.ConnectionString,
            TableDefinition = new DbTableDefinition()
            {
                TableName = DwhBuilder.ConnectionString.Escape(table.TempTableName),
                Columns = tempColumns
                    .ToDictionary(x => x.Name, x => DwhBuilder.ConnectionString.Escape(x.Name)),
            },
        };
    }

    private IEnumerable<IExecutable> CreateTableMainProcess()
    {
        var mutators = new MutatorList();
        foreach (var creator in _mutatorCreators)
        {
            mutators.Add(creator?.Invoke(this));
        }

        mutators.Add(CreateTempWriter(ResilientTable, Table));

        DateTimeOffset? maxRecordTimestamp = null;
        if (DwhBuilder.Configuration.IncrementalLoadEnabled && Table.GetRecordTimestampIndicatorColumn() != null)
        {
            maxRecordTimestamp = GetMaxRecordTimestamp();
        }

        var inputProcess = _inputProcessCreator?.Invoke(maxRecordTimestamp);

        yield return new ProcessBuilder()
        {
            InputProcess = inputProcess,
            Mutators = mutators,
        }.Build();
    }

    private DateTimeOffset? GetMaxRecordTimestamp()
    {
        var recordTimestampIndicatorColumn = Table.GetRecordTimestampIndicatorColumn();
        if (recordTimestampIndicatorColumn == null)
            return null;

        var result = new GetTableMaxValue<object>(ResilientTable.Scope.Context)
        {
            Name = nameof(GetMaxRecordTimestamp) + "Reader",
            ConnectionString = ResilientTable.Scope.ConnectionString,
            TableName = ResilientTable.TableName,
            ColumnName = recordTimestampIndicatorColumn.NameEscaped(ResilientTable.Scope.ConnectionString),
        }.Execute(ResilientTable.Scope);

        if (result == null)
            return null;

        if (result.MaxValue == null)
        {
            if (result.RecordCount > 0)
                return DwhBuilder.Configuration.InfinitePastDateTime;

            return null;
        }

        if (result.MaxValue is DateTime dt)
        {
            return new DateTimeOffset(dt, TimeSpan.Zero);
        }

        return (DateTimeOffset)result.MaxValue;
    }

    private void CreateTableFinalizers(ResilientSqlTableTableFinalizerBuilder builder)
    {
        foreach (var creator in _finalizerCreators)
        {
            builder.Finalizers.AddRange(creator.Invoke(this));
        }
    }
}
