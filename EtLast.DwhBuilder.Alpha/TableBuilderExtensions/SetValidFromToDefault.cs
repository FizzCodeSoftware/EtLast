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
                    new CustomMutator(builder.DwhBuilder.Context, nameof(SetValidFromToDefault), builder.Topic)
                    {
                        Then = (proc, row) =>
                        {
                            row.SetValue(builder.ValidFromColumnName, builder.DwhBuilder.DefaultValidFromDateTime, proc);
                            return true;
                        },
                    },
                });
            }

            return builders;
        }
    }
}