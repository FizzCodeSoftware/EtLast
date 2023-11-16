﻿namespace FizzCode.EtLast.DwhBuilder.MsSql;

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

    private static IEnumerable<IProcess> CreateDisableConstraintCheckFinalizer(DwhTableBuilder builder)
    {
        if (builder.Table.ForeignKeys.Count == 0)
            yield break;

        var tableNames = !builder.Table.GetHasHistoryTable()
                ? new[] { builder.ResilientTable.TableName }
                : [builder.ResilientTable.TableName, builder.DwhBuilder.GetEscapedHistTableName(builder.Table)];

        yield return new MsSqlDisableConstraintCheck(builder.ResilientTable.Scope.Context)
        {
            Name = "DisableConstraintCheck",
            ConnectionString = builder.ResilientTable.Scope.ConnectionString,
            TableNames = tableNames,
            CommandTimeout = 60 * 60,
        };

        yield return new CustomJob(builder.ResilientTable.Scope.Context)
        {
            Name = "UpdateConstraintList",
            Action = process =>
            {
                var list = builder.DwhBuilder.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
                if (list == null)
                {
                    list = [];
                    builder.DwhBuilder.Context.AdditionalData["ConstraintCheckDisabledOnTables"] = list;
                }

                list.AddRange(tableNames);
            }
        };
    }
}
