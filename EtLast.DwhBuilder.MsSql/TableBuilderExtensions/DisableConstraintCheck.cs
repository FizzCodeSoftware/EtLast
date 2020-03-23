namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System.Collections.Generic;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] DisableConstraintCheck(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(CreateDisableConstraintCheckFinalizer);
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateDisableConstraintCheckFinalizer(DwhTableBuilder builder)
        {
            if (builder.Table.ForeignKeys.Count == 0)
                yield break;

            var hasHistoryTable = builder.Table.GetHasHistoryTable();

            yield return new MsSqlDisableConstraintCheck(builder.ResilientTable.Topic, "DisableConstraintCheck")
            {
                ConnectionString = builder.ResilientTable.Scope.Configuration.ConnectionString,
                TableNames = !hasHistoryTable
                    ? new[] { builder.ResilientTable.TableName }
                    : new[] { builder.ResilientTable.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.Table) },
                CommandTimeout = 60 * 60,
            };

            yield return new CustomAction(builder.ResilientTable.Topic, "UpdateConstraintList")
            {
                Then = process =>
                {
                    var list = builder.DwhBuilder.Topic.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
                    if (list == null)
                    {
                        list = new List<string>();
                        builder.DwhBuilder.Topic.Context.AdditionalData["ConstraintCheckDisabledOnTables"] = list;
                    }

                    list.AddRange(!hasHistoryTable
                        ? new[] { builder.ResilientTable.TableName }
                        : new[] { builder.ResilientTable.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.Table) });
                }
            };
        }
    }
}