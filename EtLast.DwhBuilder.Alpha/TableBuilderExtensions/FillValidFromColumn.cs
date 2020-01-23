namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;

    public static class FillValidFromColumnExtension
    {
        public static DwhTableBuilder[] FillValidFromColumn(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                var hasHistory = !builder.SqlTable.HasProperty<NoHistoryTableProperty>();
                if (hasHistory)
                {
                    builder.AddOperationCreator(builder => new[]
                    {
                        new CustomOperation()
                        {
                            InstanceName = "FillValidFrom",
                            Then = (op, row) =>
                            {
                                if (!string.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName) && !row.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName))
                                {
                                    row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, row.GetAs<DateTime>(builder.DwhBuilder.Configuration.LastModifiedColumnName), op);
                                }
                                else
                                {
                                    row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.InfinitePastDateTime, op);
                                }
                            },
                        },
                    });
                }
            }

            return builders;
        }
    }
}