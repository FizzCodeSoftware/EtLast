namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToRecordTimestampIfAvailable(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (string.IsNullOrEmpty(builder.ValidFromColumnName))
                    continue;

                if (builder.RecordTimestampIndicatorColumn == null)
                    continue;

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
                                    op.Process.Context.Log(LogSeverity.Warning, op.Process, op, "record timestamp is not DateTimeOffset in: {Row}", row.ToDebugString());
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