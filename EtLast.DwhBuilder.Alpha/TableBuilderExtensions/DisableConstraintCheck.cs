namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using FizzCode.DbTools.DataDefinition;
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
            if (!builder.SqlTable.HasProperty<ForeignKey>())
                yield break;

            var hasHistoryTable = builder.SqlTable.HasProperty<WithHistoryTableProperty>();

            yield return new MsSqlDisableConstraintCheckProcess(builder.Table.Topic, "DisableConstraintCheck")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                TableNames = !hasHistoryTable
                    ? new[] { builder.Table.TableName }
                    : new[] { builder.Table.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.SqlTable) },
                CommandTimeout = 60 * 60,
            };

            yield return new CustomActionProcess(builder.Table.Topic, "UpdateConstraintList")
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
                        ? new[] { builder.Table.TableName }
                        : new[] { builder.Table.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.SqlTable) });
                }
            };
        }
    }
}