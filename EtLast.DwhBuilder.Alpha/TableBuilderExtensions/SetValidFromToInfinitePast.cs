namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToDefault(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => new[]
                {
                    new CustomOperation()
                    {
                        InstanceName = nameof(SetValidFromToDefault),
                        Then = (op, row) =>
                        {
                            row.SetValue(builder.ValidFromColumnName, builder.DwhBuilder.DefaultValidFromDateTime, op);
                        },
                    },
                });
            }

            return builders;
        }
    }
}