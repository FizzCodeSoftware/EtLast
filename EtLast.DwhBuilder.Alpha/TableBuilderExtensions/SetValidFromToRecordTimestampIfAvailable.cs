namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToRecordTimestampIfAvailable(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (builder.RecordTimestampIndicatorColumn == null)
                    throw new NotSupportedException();

                /*var hasHistory = !builder.SqlTable.HasProperty<NoHistoryTableProperty>();
                if (!hasHistory)
                    continue;*/

                builder.AddOperationCreator(builder => new[]
                {
                    new CustomOperation()
                    {
                        InstanceName = nameof(SetValidFromToRecordTimestampIfAvailable),
                        Then = (op, row) =>
                        {
                            var value = row[builder.RecordTimestampIndicatorColumn.Name];
                            if (value != null)
                            {
                                if (value is DateTime dt)
                                {
                                    value = (DateTimeOffset)dt;
                                }

                                if (value is DateTimeOffset)
                                {
                                    row.SetValue(builder.ValidFromColumnName, value, op);
                                }
                                else
                                {
                                    op.Process.Context.Log(LogSeverity.Warning, op.Process, op, "record timestamp is not DateTimeOffset in: {Row}", row.ToString());
                                }
                            }
                        },
                    },
                });
            }

            return builders;
        }
    }
}