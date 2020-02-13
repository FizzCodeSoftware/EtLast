namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public class DwhBuilder
    {
        public IEtlContext Context { get; }

        public DatabaseDefinition Model { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }

        private DwhConfiguration _configuration;
        public DwhConfiguration Configuration { get => _configuration; set => SetConfiguration(value); }

        public IEnumerable<SqlTable> Tables => _tables.Select(x => x.SqlTable);
        protected readonly List<DwhTableBuilder> _tables = new List<DwhTableBuilder>();

        public DateTimeOffset? DefaultValidFromDateTime => Configuration.UseContextCreationTimeForNewRecords ? Context.CreatedOnLocal : Configuration.InfinitePastDateTime;

        public DwhBuilder(IEtlContext context)
        {
            Context = context;
        }

        private void SetConfiguration(DwhConfiguration configuration)
        {
            _tables.Clear();
            _configuration = configuration;
        }

        public IExecutable Build(string scopeName = null, string scopeTopic = null)
        {
            if (Configuration == null)
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
                throw new ArgumentNullException(nameof(Configuration));
#pragma warning restore CA2208 // Instantiate argument exceptions correctly

            foreach (var tableBuilder in _tables)
            {
                tableBuilder.Build();
            }

            return new ResilientSqlScope(Context, scopeName, scopeTopic)
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

        private IEnumerable<IExecutable> CreatePostFinalizers(ResilientSqlScope scope, IProcess caller)
        {
            // todo: this should be built and configured by DisableConstraintCheck
            var constraintCheckDisabledOnTables = scope.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
            if (constraintCheckDisabledOnTables != null)
            {
                yield return new MsSqlEnableConstraintCheckProcess(Context, "EnableConstraintCheck", scope.Topic)
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    TableNames = constraintCheckDisabledOnTables.Distinct().OrderBy(x => x).ToArray(),
                    CommandTimeout = 60 * 60,
                };
            }

            var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunInfoTableProperty>());
            if (etlRunSqlTable != null)
            {
                yield return new CustomSqlStatementProcess(Context, "UpdateEtlRun", etlRunSqlTable.SchemaAndTableName.SchemaAndName)
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SqlStatement = "UPDATE " + ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema)
                        + " SET FinishedOn = @FinishedOn, Result = @Result"
                        + " WHERE EtlRunId = @EtlRunId",
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
            var hasHistory = sqlTable.HasProperty<WithHistoryTableProperty>();
            if (!hasHistory)
                return null;

            return ConnectionString.Escape(sqlTable.SchemaAndTableName.TableName + "Hist", sqlTable.SchemaAndTableName.Schema);
        }

        private IEnumerable<IExecutable> CreateInitializers(ResilientSqlScope scope, IProcess caller)
        {
            var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunInfoTableProperty>());
            if (etlRunSqlTable != null)
            {
                var maxId = new GetTableMaxValueProcess<int>(Context, "MaxIdReader", etlRunSqlTable.SchemaAndTableName.SchemaAndName)
                {
                    ConnectionString = ConnectionString,
                    TableName = ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema),
                    ColumnName = ConnectionString.Escape("EtlRunId"),
                }.Execute(caller);

                yield return new ProcessBuilder()
                {
                    InputProcess = new EnumerableImportProcess(Context, "RowCreator", etlRunSqlTable.SchemaAndTableName.SchemaAndName)
                    {
                        InputGenerator = process =>
                        {
                            var currentId = (maxId?.MaxValue ?? 0) + 1;
                            Context.AdditionalData["CurrentEtlRunId"] = currentId;

                            var initialValues = new Dictionary<string, object>()
                            {
                                ["EtlRunId"] = currentId,
                                ["MachineName"] = Environment.MachineName,
                                ["UserName"] = Environment.UserName,
                                ["StartedOn"] = Context.CreatedOnLocal,
                            };

                            return new[] { Context.CreateRow(process, initialValues) };
                        }
                    },
                    Mutators = new MutatorList()
                    {
                        new MsSqlWriteToTableWithMicroTransactionsMutator(Context, "Writer", etlRunSqlTable.SchemaAndTableName.SchemaAndName)
                        {
                            ConnectionString = ConnectionString,
                            TableDefinition = new DbTableDefinition()
                            {
                                TableName = ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema),
                                Columns = new[] { "EtlRunId", "MachineName", "UserName", "StartedOn" }
                                    .Select(c => new DbColumnDefinition(c, ConnectionString.Escape(c)))
                                    .ToArray(),
                            },
                        },
                    },
                }.Build();
            }
        }

        public DwhTableBuilder[] AddTables(params SqlTable[] sqlTables)
        {
            var result = new DwhTableBuilder[sqlTables.Length];

            for (var i = 0; i < sqlTables.Length; i++)
            {
                var sqlTable = sqlTables[i];

                var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
                var pkIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());

                var tempColumns = sqlTable.Columns
                    .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

                if (pkIsIdentity)
                {
                    tempColumns = tempColumns
                        .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
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