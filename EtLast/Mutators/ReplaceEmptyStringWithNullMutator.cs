namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ReplaceEmptyStringWithNullMutator : AbstractMutator
    {
        public string[] Columns { get; init; }

        public ReplaceEmptyStringWithNullMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    if ((row[column] as string) == string.Empty)
                    {
                        row.SetStagedValue(column, null);
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    if ((kvp.Value as string) == string.Empty)
                    {
                        row.SetStagedValue(kvp.Key, null);
                    }
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ReplaceEmptyStringWithNullMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceEmptyStringWithNull(this IFluentProcessMutatorBuilder builder, ReplaceEmptyStringWithNullMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}