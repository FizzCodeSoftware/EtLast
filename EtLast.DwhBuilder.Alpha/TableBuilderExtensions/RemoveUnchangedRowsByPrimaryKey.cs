namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.EtLast.AdoNet;

    public static class RemoveUnchangedRowsByPrimaryKeyExtension
    {
        public static DwhTableBuilder[] RemoveUnchangedRowsByPrimaryKey(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                var primaryKey = builder.SqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
                if (primaryKey == null || primaryKey.SqlColumns.Count != 1)
                    throw new NotSupportedException();

                var pkCol = primaryKey.SqlColumns[0].SqlColumn;

                var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();

                var columnsToCompare = builder.SqlTable.Columns.Where(x => x.Name != pkCol.Name);
                if (useEtlRunTable)
                {
                    columnsToCompare = columnsToCompare.Where(x => x.Name != builder.DwhBuilder.Configuration.EtlInsertRunIdColumnName && x.Name != builder.DwhBuilder.Configuration.EtlUpdateRunIdColumnName);
                }

                // todo: should ValidFrom be excluded from the list??

                if (pkCol.Type.IsInt())
                {
                    builder.AddOperationCreator(builder => new[] {
                    new DeferredCompareWithRowOperation()
                    {
                        InstanceName = "RemoveUnchangedRows",
                        MatchAction = new MatchAction(MatchMode.Remove),
                        LeftKeySelector = row => row.GetAs<int>(pkCol.Name).ToString("D", CultureInfo.InvariantCulture),
                        RightKeySelector = row => row.GetAs<int>(pkCol.Name).ToString("D", CultureInfo.InvariantCulture),
                        RightProcessCreator = rows => new AdoNetDbReaderProcess(builder.Table.Scope.Context, "ExistingRecordReader")
                        {
                            ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                            TableName = builder.Table.TableName,
                            InlineArrayParameters = true,
                            CustomWhereClause = pkCol.Name + " IN (@idList)",
                            Parameters = new Dictionary<string, object>
                            {
                                ["idList"] = rows.Select(row => row.GetAs<int>(pkCol.Name)).Distinct().ToArray(),
                            },
                        },
                        EqualityComparer = new ColumnBasedRowEqualityComparer()
                        {
                            Columns = columnsToCompare.Select(x => x.Name).ToArray(),
                        }
                    },
                });
                }
                else if (pkCol.Type.AnyOf(MsSqlType2016.Char, MsSqlType2016.NVarChar, MsSqlType2016.Char, MsSqlType2016.VarChar))
                {
                    builder.AddOperationCreator(builder => new[] {
                    new DeferredCompareWithRowOperation()
                    {
                        InstanceName = "RemoveUnchangedRows",
                        MatchAction = new MatchAction(MatchMode.Remove),
                        LeftKeySelector = row => row.GetAs<string>(pkCol.Name),
                        RightKeySelector = row => row.GetAs<string>(pkCol.Name),
                        RightProcessCreator = rows => new AdoNetDbReaderProcess(builder.Table.Scope.Context, "ExistingRecordReader")
                        {
                            ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                            TableName = builder.Table.TableName,
                            InlineArrayParameters = true,
                            CustomWhereClause = pkCol.Name + " IN (@idList)",
                            Parameters = new Dictionary<string, object>
                            {
                                ["idList"] = rows.Select(row => row.GetAs<string>(pkCol.Name)).Distinct().ToArray(),
                            },
                        },
                        EqualityComparer = new ColumnBasedRowEqualityComparer()
                        {
                            Columns = columnsToCompare.Select(x => x.Name).ToArray(),
                        }
                    }
                });
                }
            }

            return builders;
        }
    }
}