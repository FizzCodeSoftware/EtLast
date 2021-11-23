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
                    new CustomMutator(builder.ResilientTable.Scope.Context)
                    {
                        Name = nameof(SetValidFromToRecordTimestampIfAvailable),
                        Action = row =>
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
                                    row[builder.ValidFromColumn.Name] = value;
                                }
                                else
                                {
                                    row.CurrentProcess.Context.Log(LogSeverity.Warning, row.CurrentProcess, "record timestamp is not DateTimeOffset in: {Row}", row.ToDebugString());
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