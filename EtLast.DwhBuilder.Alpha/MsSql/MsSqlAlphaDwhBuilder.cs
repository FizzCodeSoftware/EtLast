namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public class MsSqlAlphaDwhBuilder : IAlphaDwhBuilder
    {
        public IEtlContext Context { get; }

        public DatabaseDefinition Model { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }

        private AlphaDwhConfiguration _configuration;
        public AlphaDwhConfiguration Configuration { get => _configuration; set => SetConfiguration(value); }

        public CommonOperationsCreatorDelegate CommonOperationsBeforeTables { get; set; }
        public CommonOperationsCreatorDelegate CommonOperationsAfterTables { get; set; }

        public IEnumerable<SqlTable> Tables => _tables.Select(x => x.Item2);
        protected readonly List<Tuple<ResilientTable, SqlTable>> _tables = new List<Tuple<ResilientTable, SqlTable>>();

        public MsSqlAlphaDwhBuilder(IEtlContext context)
        {
            Context = context;
        }

        private void SetConfiguration(AlphaDwhConfiguration configuration)
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

            return new ResilientSqlScope(Context, name)
            {
                Configuration = new ResilientSqlScopeConfiguration()
                {
                    ConnectionString = ConnectionString,
                    TempTableMode = Configuration.TempTableMode,
                    Tables = _tables.Select(x => x.Item1).ToList(),
                    InitializerCreator = CreateInitializers,
                    FinalizerRetryCount = Configuration.FinalizerRetryCount,
                    FinalizerTransactionScopeKind = TransactionScopeKind.RequiresNew,
                    PostFinalizerCreator = CreatePostFinalizers,
                },
            };
        }

        private IEnumerable<IExecutable> CreatePostFinalizers(ResilientSqlScope scope)
        {
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
                var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunTableProperty>());

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

        protected string GetEscapedTempTableName(SqlTable sqlTable)
        {
            return ConnectionString.Escape(Configuration.TempTableNamePrefix + sqlTable.SchemaAndTableName.TableName, sqlTable.SchemaAndTableName.Schema);
        }

        protected string GetEscapedHistTableName(SqlTable sqlTable)
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
                var etlRunSqlTable = Model.GetTables().Find(x => x.HasProperty<IsEtlRunTableProperty>());

                yield return new OperationHostProcess(Context, "EtlRunIdCreator")
                {
                    InputProcess = new CustomSqlAdoNetDbReaderProcess(Context, "MaxEtlRunIdReader")
                    {
                        ConnectionString = ConnectionString,
                        Sql = "select max(EtlRunId) as MaxEtlRunId, count(*) as RunCount from " + ConnectionString.Escape(etlRunSqlTable.SchemaAndTableName.TableName, etlRunSqlTable.SchemaAndTableName.Schema),
                        SuppressExistingTransactionScope = true,
                    },
                    Operations = new List<IRowOperation>()
                    {
                        new CustomOperation()
                        {
                            Then = (op, row) =>
                            {
                                var maxId = row.IsNull("RunCount") || row.IsNull("MaxEtlRunId") ? 0 : row.GetAs<int>("MaxEtlRunId");
                                var currentId = maxId + 1;
                                Context.AdditionalData["CurrentEtlRunId"] = currentId;

                                row.SetValue("EtlRunId", currentId, op);
                                row.SetValue("MachineName", Environment.MachineName, op);
                                row.SetValue("UserName", Environment.UserName, op);
                                row.SetValue("StartedOn", DateTimeOffset.Now, op);
                            },
                        },
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

        public ResilientTable AddTable(SqlTable sqlTable, InputProcessCreatorDelegate inputProcessCreator, IEnumerable<IRowOperation> tableSpecificOperations = null)
        {
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk.SqlColumns.Count != 1)
                throw new ArgumentException(nameof(AddTable) + " can be used only for tables with a single-column primary key (table name: " + sqlTable.SchemaAndTableName.SchemaAndName + ")");

            var tempColumns = sqlTable.Columns.Select(x => x.Name);
            if (Configuration.UseEtlRunTable)
            {
                tempColumns = tempColumns
                    .Where(x => x != Configuration.EtlInsertRunIdColumnName && x != Configuration.EtlUpdateRunIdColumnName);
            }

            var table = new ResilientTable()
            {
                TableName = ConnectionString.Escape(sqlTable.SchemaAndTableName.TableName, sqlTable.SchemaAndTableName.Schema),
                TempTableName = GetEscapedTempTableName(sqlTable),
                Columns = tempColumns.ToArray(),
                MainProcessCreator = t => CreateTableMainProcess(sqlTable, t, tableSpecificOperations, inputProcessCreator.Invoke(this, t, sqlTable)),
                FinalizerCreator = t => CreateTableFinalizer(sqlTable, t),
            };

            _tables.Add(new Tuple<ResilientTable, SqlTable>(table, sqlTable));
            return table;
        }

        protected IEnumerable<IExecutable> CreateTableFinalizer(SqlTable sqlTable, ResilientTable t)
        {
            var hasHistory = !sqlTable.HasProperty<NoHistoryTableProperty>();
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var pkColumnName = pk.SqlColumns[0].SqlColumn.Name;

            IEnumerable<SqlColumn> tempColumns = sqlTable.Columns;
            if (Configuration.UseEtlRunTable)
            {
                tempColumns = tempColumns.Where(x => x.Name != Configuration.EtlInsertRunIdColumnName && x.Name != Configuration.EtlUpdateRunIdColumnName);
            }

            var allColumnsExceptValidTo = tempColumns.Where(x => x.Name != Configuration.ValidToColumnName).ToList();
            var headColumnsToUpdate = tempColumns.Where(x => x.Name != Configuration.ValidToColumnName).ToList();
            var headColumnsToInsert = tempColumns.Where(x => x.Name != Configuration.ValidToColumnName).ToList();
            var noHistoryColumns = tempColumns.Where(x => x.HasProperty<NoHistoryColumnProperty>()).ToList();

            var currentRunid = Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            yield return new MsSqlDisableConstraintCheckProcess(Context, "DisableConstraintCheck")
            {
                ConnectionString = t.Scope.Configuration.ConnectionString,
                TableNames = !hasHistory
                    ? new[] { t.TableName }
                    : new[] { t.TableName, GetEscapedHistTableName(sqlTable) },
                CommandTimeout = 60 * 60,
            };

            yield return new CustomActionProcess(Context, "UpdateConstraintList")
            {
                Then = process =>
                {
                    var list = Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
                    if (list == null)
                    {
                        list = new List<string>();
                        Context.AdditionalData["ConstraintCheckDisabledOnTables"] = list;
                    }

                    list.AddRange(!hasHistory
                        ? new[] { t.TableName }
                        : new[] { t.TableName, GetEscapedHistTableName(sqlTable) });
                }
            };

            var useEtlRunTable = Configuration.UseEtlRunTable && !sqlTable.HasProperty<NoEtlRunColumnsProperty>();

            yield return new CustomMsSqlMergeSqlStatementProcess(Context, "MergeIntoBase")
            {
                ConnectionString = t.Scope.Configuration.ConnectionString,
                CommandTimeout = 60 * 60,
                SourceTableName = t.TempTableName,
                SourceTableAlias = "s",
                TargetTableName = t.TableName,
                TargetTableAlias = "t",
                OnCondition = "t." + ConnectionString.Escape(pkColumnName) + " = s." + ConnectionString.Escape(pkColumnName),
                WhenMatchedAction = "UPDATE SET " + string.Join(", ", headColumnsToUpdate.Select(c => "t." + ConnectionString.Escape(c.Name) + "=s." + ConnectionString.Escape(c.Name)))
                                    + (useEtlRunTable ? ", " + Configuration.EtlUpdateRunIdColumnName + "=" + currentRunid.ToString("D", CultureInfo.InvariantCulture) : ""),
                WhenNotMatchedByTargetAction = "INSERT (" + string.Join(", ", headColumnsToInsert.Select(c => ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + Configuration.EtlInsertRunIdColumnName + ", " + Configuration.EtlUpdateRunIdColumnName : "")
                    + ") VALUES (" + string.Join(", ", headColumnsToInsert.Select(c => "s." + ConnectionString.Escape(c.Name)))
                    + (useEtlRunTable ? ", " + currentRunid.ToString("D", CultureInfo.InvariantCulture) + ", " + currentRunid.ToString("D", CultureInfo.InvariantCulture) : "")
                    + ")",
            };

            if (hasHistory)
            {
                var histTableName = GetEscapedHistTableName(sqlTable);

                yield return new CustomMsSqlMergeSqlStatementProcess(Context, "CloseLastHistIntervals")
                {
                    ConnectionString = t.Scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    SourceTableName = t.TempTableName,
                    SourceTableAlias = "s",
                    TargetTableName = histTableName,
                    TargetTableAlias = "t",
                    OnCondition = "t." + ConnectionString.Escape(pkColumnName) + " = s." + ConnectionString.Escape(pkColumnName) + " and t." + ConnectionString.Escape(Configuration.ValidToColumnName) + " = @InfiniteFuture",
                    WhenMatchedAction = "UPDATE SET t." + ConnectionString.Escape(Configuration.ValidToColumnName) + "=s." + ConnectionString.Escape(Configuration.ValidFromColumnName)
                                        + (useEtlRunTable ? ", " + Configuration.EtlUpdateRunIdColumnName + "=" + currentRunid.ToString("D", CultureInfo.InvariantCulture) : ""),
                    Parameters = new Dictionary<string, object>
                    {
                        ["InfiniteFuture"] = Configuration.InfiniteFutureDateTime,
                    },
                };

                if (noHistoryColumns.Count > 0)
                {
                    yield return new CustomMsSqlMergeSqlStatementProcess(Context, "UpdateNoHistory")
                    {
                        ConnectionString = t.Scope.Configuration.ConnectionString,
                        CommandTimeout = 60 * 60,
                        SourceTableName = t.TempTableName,
                        SourceTableAlias = "s",
                        TargetTableName = histTableName,
                        TargetTableAlias = "t",
                        OnCondition = "t." + ConnectionString.Escape(pkColumnName) + " = s." + ConnectionString.Escape(pkColumnName),
                        WhenMatchedAction = "UPDATE SET " + string.Join(", ", noHistoryColumns.Select(col => "t." + ConnectionString.Escape(col.Name) + " = s." + ConnectionString.Escape(col.Name)))
                                            + (useEtlRunTable ? ", " + Configuration.EtlUpdateRunIdColumnName + "=" + currentRunid.ToString("D", CultureInfo.InvariantCulture) : ""),
                    };
                }

                Dictionary<string, object> columnDefaults = null;
                if (Configuration.UseEtlRunTable)
                {
                    columnDefaults = new Dictionary<string, object>
                    {
                        [Configuration.EtlInsertRunIdColumnName] = currentRunid,
                        [Configuration.EtlUpdateRunIdColumnName] = currentRunid
                    };
                }

                yield return new CopyTableIntoExistingTableProcess(Context, "CopyToHist")
                {
                    ConnectionString = t.Scope.Configuration.ConnectionString,
                    Configuration = new TableCopyConfiguration()
                    {
                        SourceTableName = t.TempTableName,
                        TargetTableName = histTableName,
                        ColumnConfiguration = allColumnsExceptValidTo.Select(x => new ColumnCopyConfiguration(x.Name)).ToList(),
                    },
                    ColumnDefaults = columnDefaults,
                    CommandTimeout = 60 * 60,
                };
            }
        }

        protected IEnumerable<IExecutable> CreateTableMainProcess(SqlTable sqlTable, ResilientTable table, IEnumerable<IRowOperation> tableSpecificOperations, IEvaluable reader)
        {
            var operations = new List<IRowOperation>();

            var hasHistory = !sqlTable.HasProperty<NoHistoryTableProperty>();
            if (hasHistory)
            {
                operations.Add(new CustomOperation()
                {
                    InstanceName = "CopyLastModifiedToValidFrom",
                    Then = (op, row) =>
                    {
                        if (!row.IsNullOrEmpty(Configuration.LastModifiedColumnName))
                        {
                            row.SetValue(Configuration.ValidFromColumnName, row.GetAs<DateTime>(Configuration.LastModifiedColumnName), op);
                        }
                        else
                        {
                            row.SetValue(Configuration.ValidFromColumnName, Configuration.InfinitePastDateTime, op);
                        }
                    },
                });
            }

            if (CommonOperationsBeforeTables != null)
            {
                operations.AddRange(CommonOperationsBeforeTables.Invoke(this, table, sqlTable));
            }

            if (tableSpecificOperations != null)
            {
                operations.AddRange(tableSpecificOperations);
            }

            if (CommonOperationsAfterTables != null)
            {
                operations.AddRange(CommonOperationsAfterTables.Invoke(this, table, sqlTable));
            }

            var convertBoolOp = CreateConvertBoolOperation(sqlTable);
            if (convertBoolOp != null)
                operations.Add(convertBoolOp);

            operations.Add(RemoveUnchangedRowsOperation(table, sqlTable));

            var currentRunid = Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            operations.Add(CreateTempWriterOperation(table, sqlTable));

            yield return new OperationHostProcess(table.Scope.Context, ConnectionString.Unescape(table.TableName))
            {
                InputProcess = reader,
                Operations = operations,
            };
        }

        private IRowOperation CreateTempWriterOperation(ResilientTable table, SqlTable sqlTable, bool bulkCopyCheckConstraints = false)
        {
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var isIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());

            var tempColumns = sqlTable.Columns.Select(x => x.Name);
            if (isIdentity)
            {
                var pkColumnNames = pk.SqlColumns.Select(x => x.SqlColumn.Name).ToHashSet();
                tempColumns = tempColumns.Where(x => !pkColumnNames.Contains(x));
            }

            if (Configuration.UseEtlRunTable)
            {
                tempColumns = tempColumns.Where(x => x != Configuration.EtlInsertRunIdColumnName && x != Configuration.EtlUpdateRunIdColumnName);
            }

            return new MsSqlWriteToTableWithMicroTransactionsOperation()
            {
                InstanceName = "Write",
                ConnectionString = table.Scope.Configuration.ConnectionString,
                BulkCopyCheckConstraints = bulkCopyCheckConstraints,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = tempColumns
                    .Select(column => new DbColumnDefinition(column))
                    .ToArray(),
                },
            };
        }

        private static IRowOperation CreateConvertBoolOperation(SqlTable sqlTable)
        {
            var boolColumns = sqlTable.Columns
                .Where(col => col.Type == SqlType.Boolean)
                .Select(col => col.Name)
                .ToList();

            if (boolColumns.Count == 0)
                return null;

            return new CustomOperation()
            {
                InstanceName = "ConvertBooleans",
                Then = (op, row) =>
                {
                    foreach (var col in boolColumns)
                    {
                        if (!row.IsNull(col))
                        {
                            var value = row[col];
                            if (value is bool boolv)
                                row.SetValue(col, boolv, op);
                            else if (value is byte bv)
                                row.SetValue(col, bv == 1, op);
                            else if (value is int iv)
                                row.SetValue(col, iv == 1, op);
                            else
                                throw new InvalidValueException(op.Process, null, row, col);
                        }
                    }
                }
            };
        }

        private static IRowOperation RemoveUnchangedRowsOperation(ResilientTable table, SqlTable sqlTable)
        {
            var primaryKey = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (primaryKey == null || primaryKey.SqlColumns.Count != 1)
                throw new NotSupportedException();

            var pkCol = primaryKey.SqlColumns[0].SqlColumn;

            return new DeferredCompareWithRowOperation()
            {
                InstanceName = "RemoveUnchangedRows",
                MatchAction = new MatchAction(MatchMode.Remove),
                LeftKeySelector = row => row.GetAs<int>(pkCol.Name).ToString("D", CultureInfo.InvariantCulture),
                RightKeySelector = row => row.GetAs<int>(pkCol.Name).ToString("D", CultureInfo.InvariantCulture),
                RightProcessCreator = rows => new AdoNetDbReaderProcess(table.Scope.Context, "ExistingRecordReader")
                {
                    ConnectionString = table.Scope.Configuration.ConnectionString,
                    TableName = table.TableName,
                    InlineArrayParameters = true,
                    CustomWhereClause = pkCol.Name + " IN (@idList)",
                    Parameters = new Dictionary<string, object>
                    {
                        ["idList"] = rows.Select(row => row.GetAs<int>(pkCol.Name)).Distinct().ToArray(),
                    },
                },
                EqualityComparer = new ColumnBasedRowEqualityComparer()
                {
                    Columns = sqlTable.Columns.Where(x => x.Name != pkCol.Name).Select(x => x.Name).ToArray(),
                }
            };
        }

        public DateTime? GetMaxLastModified(ResilientTable table)
        {
            var result = new GetTableMaxValueProcess<DateTime?>(Context, "MaxLastModifiedReader")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableName = table.TableName,
                ColumnName = Configuration.LastModifiedColumnName,
            }.Execute(table.Scope);

            if (result == null)
                return null;

            if (result.MaxValue == null)
            {
                if (result.RecordCount > 0)
                    return Configuration.InfinitePastDateTime;

                return null;
            }

            return result.MaxValue;
        }
    }
}