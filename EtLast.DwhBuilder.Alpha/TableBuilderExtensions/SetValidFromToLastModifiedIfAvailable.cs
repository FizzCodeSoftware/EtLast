namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToLastModifiedIfAvailable(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (string.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName))
                    throw new NotSupportedException();

                var hasHistory = !builder.SqlTable.HasProperty<NoHistoryTableProperty>();
                if (!hasHistory)
                    continue;

                builder.AddOperationCreator(builder => new[]
                {
                    new CustomOperation()
                    {
                        InstanceName = nameof(SetValidFromToLastModifiedIfAvailable),
                        Then = (op, row) =>
                        {
                            if (!string.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName)
                                && !row.IsNullOrEmpty(builder.DwhBuilder.Configuration.LastModifiedColumnName))
                            {
                                row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, row[builder.DwhBuilder.Configuration.LastModifiedColumnName], op);
                            }
                        },
                    },
                });
            }

            return builders;
        }
    }
}