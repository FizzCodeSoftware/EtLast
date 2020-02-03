namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AddConstraintCheckDisablerFinalizer(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(ConstraintCheckDisableFinalizer);
            }

            return builders;
        }

        private static IEnumerable<IExecutable> ConstraintCheckDisableFinalizer(DwhTableBuilder builder)
        {
            if (!builder.SqlTable.HasProperty<ForeignKey>())
                yield break;

            var hasHistory = !builder.SqlTable.HasProperty<NoHistoryTableProperty>();

            yield return new MsSqlDisableConstraintCheckProcess(builder.DwhBuilder.Context, "DisableConstraintCheck")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                TableNames = !hasHistory
                    ? new[] { builder.Table.TableName }
                    : new[] { builder.Table.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.SqlTable) },
                CommandTimeout = 60 * 60,
            };

            yield return new CustomActionProcess(builder.DwhBuilder.Context, "UpdateConstraintList")
            {
                Then = process =>
                {
                    var list = builder.DwhBuilder.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
                    if (list == null)
                    {
                        list = new List<string>();
                        builder.DwhBuilder.Context.AdditionalData["ConstraintCheckDisabledOnTables"] = list;
                    }

                    list.AddRange(!hasHistory
                        ? new[] { builder.Table.TableName }
                        : new[] { builder.Table.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.SqlTable) });
                }
            };
        }
    }
}