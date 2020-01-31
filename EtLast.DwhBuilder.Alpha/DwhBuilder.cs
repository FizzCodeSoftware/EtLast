namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public delegate IEnumerable<IRowOperation> CommonOperationsCreatorDelegate(DwhTableBuilder tableBuilder);

    public class DwhBuilder
    {
        public IEtlContext Context { get; }

        public DatabaseDefinition Model { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }

        private DwhConfiguration _configuration;
        public DwhConfiguration Configuration { get => _configuration; set => SetConfiguration(value); }

        public IEnumerable<SqlTable> Tables => _tables.Select(x => x.SqlTable);
        protected readonly List<DwhTableBuilder> _tables = new List<DwhTableBuilder>();

        public DwhBuilder(IEtlContext context)
        {
            Context = context;
        }

        private void SetConfiguration(DwhConfiguration configuration)
        {
            _tables.Clear();
            _configuration = configuration;
        }

        public IExecutable Build(string name = null)
        {
            if (Configuration == null)
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
                throw new ArgumentNullException(nameof(Configuration));
#pragma warning restore CA2208 // Instantiate argument exceptions correctly

            foreach (var tableBuilder in _tables)
            {
                tableBuilder.Build();
            }

            return new ResilientSqlScope(Context, name)
            {
                Configuration = new ResilientSqlScopeConfiguration()
                {
                    ConnectionString = ConnectionString,
                    TempTableMode = Configuration.TempTableMode,
                    Tables = _tables.Select(x => x.Table).ToList(),
                    InitializerCreator = CreateInitializers,
                    FinalizerRetryCount = Configuration.FinalizerRetryCount,
                    FinalizerTransactionScopeKind = TransactionScopeKind.RequiresNew,
                    PostFinalizerCreator = CreatePostFinalizers,
                },
            };
        }

        private IEnumerable<IExecutable> CreatePostFinalizers(ResilientSqlScope scope)
        {
            // todo: this should be built and configured by AddConstraintCheckDisablerFinalizer
            var constraintCheckDisabledOnTables = scope.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
            if (constraintCheckDisabledOnTables != null)
            {
                yield return new MsSqlEnableConstraintCheckProcess(Context, "EnableConstraintCheckOnAllTables")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    TableNames = constraintCheckDisabledOnTables.Distinct().OrderBy(x => x).ToArray(),
                    CommandTimeout = 60 * 60,
                };
            }

            if (Configuration.UseEtlRunTable)
            {
                var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunInfoTableProperty>());

                yield return new CustomSqlStatementProcess(Context, "UpdateEtlRun")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SqlStatement = "update " + ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema) + " set FinishedOn = @FinishedOn, Result = @Result where EtlRunId = @EtlRunId",
                    Parameters = new Dictionary<string, object>
                    {
                        ["FinishedOn"] = DateTimeOffset.Now,
                        ["Result"] = "success",
                        ["EtlRunid"] = Context.AdditionalData.GetAs("CurrentEtlRunId", 0),
                    },
                };
            }
        }

        internal string GetEscapedTempTableName(SqlTable sqlTable)
        {
            return ConnectionString.Escape(Configuration.TempTableNamePrefix + sqlTable.SchemaAndTableName.TableName, sqlTable.SchemaAndTableName.Schema);
        }

        internal string GetEscapedHistTableName(SqlTable sqlTable)
        {
            var hasHistory = !sqlTable.HasProperty<NoHistoryTableProperty>();
            if (!hasHistory)
                return null;

            return ConnectionString.Escape(sqlTable.SchemaAndTableName.TableName + "Hist", sqlTable.SchemaAndTableName.Schema);
        }

        private IEnumerable<IExecutable> CreateInitializers(ResilientSqlScope scope)
        {
            if (Configuration.UseEtlRunTable)
            {
                var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunInfoTableProperty>());

                var maxId = new GetTableMaxValueProcess<int>(Context, "MaxEtlRunIdReader")
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema),
                    ColumnName = "EtlRunId",
                }.Execute();

                yield return new OperationHostProcess(Context, "EtlRunIdWriter")
                {
                    InputProcess = new EnumerableImportProcess(Context, "EtlRunIdCreator")
                    {
                        InputGenerator = process =>
                        {
                            var mid = maxId?.MaxValue ?? 0;
                            var currentId = mid + 1;
                            Context.AdditionalData["CurrentEtlRunId"] = currentId;

                            var initialValues = new Dictionary<string, object>
                            {
                                ["EtlRunId"] = currentId,
                                ["MachineName"] = Environment.MachineName,
                                ["UserName"] = Environment.UserName,
                                ["StartedOn"] = DateTimeOffset.Now
                            };

                            return new[] { Context.CreateRow(process, initialValues) };
                        }
                    },
                    Operations = new List<IRowOperation>()
                    {
                        new MsSqlWriteToTableWithMicroTransactionsOperation()
                        {
                            ConnectionString = ConnectionString,
                            TableDefinition = new DbTableDefinition()
                            {
                                TableName = ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema),
                                Columns = new[] { "EtlRunId", "MachineName", "UserName", "StartedOn" }.Select(x => new DbColumnDefinition(x)).ToArray(),
                            },
                        },
                    },
                };
            }
        }

        public DwhTableBuilder[] AddTables(params SqlTable[] sqlTables)
        {
            var result = new DwhTableBuilder[sqlTables.Length];

            for (var i = 0; i < sqlTables.Length; i++)
            {
                var sqlTable = sqlTables[i];

                var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
                if (pk.SqlColumns.Count != 1)
                    throw new ArgumentException(nameof(AddTables) + " can be used only for tables with a single-column primary key (table name: " + sqlTable.SchemaAndTableName.SchemaAndName + ")");

                var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());

                IEnumerable<SqlColumn> tempColumns = sqlTable.Columns;

                if (Configuration.UseEtlRunTable)
                {
                    tempColumns = tempColumns
                        .Where(x => x.Name != Configuration.EtlInsertRunIdColumnName && x.Name != Configuration.EtlUpdateRunIdColumnName);
                }

                if (pkIsIdentity)
                {
                    tempColumns = tempColumns
                        .Where(x => !pk.SqlColumns.Any(pkc => pkc.SqlColumn == x));
                }

                var table = new ResilientTable()
                {
                    TableName = ConnectionString.Escape(sqlTable.SchemaAndTableName.TableName, sqlTable.SchemaAndTableName.Schema),
                    TempTableName = GetEscapedTempTableName(sqlTable),
                    Columns = tempColumns.Select(x => x.Name).ToArray(),
                };

                var tableBuilder = new DwhTableBuilder(this, table, sqlTable);

                _tables.Add(tableBuilder);
                result[i] = tableBuilder;
            }

            return result;
        }
    }
}