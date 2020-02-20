namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToDefault(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (string.IsNullOrEmpty(builder.ValidFromColumnName))
                    continue;

                builder.AddMutatorCreator(builder => new[]
                {
                    new CustomMutator(builder.Table.Topic, nameof(SetValidFromToDefault))
                    {
                        Then = (proc, row) =>
                        {
                            row.SetValue(builder.ValidFromColumnName, builder.DwhBuilder.DefaultValidFromDateTime);
                            return true;
                        },
                    },
                });
            }

            return builders;
        }
    }
}