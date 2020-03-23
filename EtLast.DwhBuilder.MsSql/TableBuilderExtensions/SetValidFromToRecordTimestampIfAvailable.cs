namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToRecordTimestampIfAvailable(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (builder.ValidFromColumn == null)
                    continue;

                var recordTimestampIndicatorColumn = builder.Table.GetRecordTimestampIndicatorColumn();
                if (recordTimestampIndicatorColumn == null)
                    continue;

                builder.AddMutatorCreator(builder => new[]
                {
                    new CustomMutator(builder.ResilientTable.Topic, nameof(SetValidFromToRecordTimestampIfAvailable))
                    {
                        Then = (proc, row) =>
                        {
                            var value = row[recordTimestampIndicatorColumn.Name];
                            if (value != null)
                            {
                                if (value is DateTime dt)
                                {
                                    value = new DateTimeOffset(dt, TimeSpan.Zero);
                                }

                                if (value is DateTimeOffset)
                                {
                                    row.SetValue(builder.ValidFromColumn.Name, value);
                                }
                                else
                                {
                                    proc.Context.Log(LogSeverity.Warning, proc, "record timestamp is not DateTimeOffset in: {Row}", row.ToDebugString());
                                }
                            }

                            return true;
                        },
                    },
                });
            }

            return builders;
        }
    }
}