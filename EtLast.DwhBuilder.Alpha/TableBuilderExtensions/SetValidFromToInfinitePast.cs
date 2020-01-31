namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToInfinitePast(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(builder => new[]
                {
                    new CustomOperation()
                    {
                        InstanceName = nameof(SetValidFromToInfinitePast),
                        Then = (op, row) =>
                        {
                            row.SetValue(builder.DwhBuilder.Configuration.ValidFromColumnName, builder.DwhBuilder.Configuration.InfinitePastDateTime, op);
                        },
                    },
                });
            }

            return builders;
        }
    }
}