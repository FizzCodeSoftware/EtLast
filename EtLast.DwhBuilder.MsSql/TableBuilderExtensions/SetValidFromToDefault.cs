namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SetValidFromToDefault(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                if (builder.ValidFromColumn == null)
                    continue;

                builder.AddMutatorCreator(builder => new[]
                {
                    new CustomMutator(builder.ResilientTable.Topic, nameof(SetValidFromToDefault))
                    {
                        Then = (proc, row) =>
                        {
                            row[builder.ValidFromColumn.Name] = builder.DwhBuilder.DefaultValidFromDateTime;
                            return true;
                        },
                    },
                });
            }

            return builders;
        }
    }
}