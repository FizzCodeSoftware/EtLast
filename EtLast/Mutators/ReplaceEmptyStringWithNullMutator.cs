namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public sealed class ReplaceEmptyStringWithNullMutator : AbstractSimpleChangeMutator
    {
        public string[] Columns { get; init; }

        public ReplaceEmptyStringWithNullMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Changes.Clear();

            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    if ((row[column] as string) == string.Empty)
                    {
                        Changes.Add(new KeyValuePair<string, object>(column, null));
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    if ((kvp.Value as string) == string.Empty)
                    {
                        Changes.Add(new KeyValuePair<string, object>(kvp.Key, null));
                    }
                }
            }

            row.MergeWith(Changes);

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