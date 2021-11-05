namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class TrimStringMutator : AbstractSimpleChangeMutator
    {
        public string[] Columns { get; init; }

        public TrimStringMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Changes.Clear();

            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    if (row[column] is string str && !string.IsNullOrEmpty(str))
                    {
                        var trimmed = str.Trim();
                        if (trimmed != str)
                        {
                            Changes.Add(new KeyValuePair<string, object>(column, trimmed));
                        }
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    if (kvp.Value is string str && !string.IsNullOrEmpty(str))
                    {
                        var trimmed = str.Trim();
                        if (trimmed != str)
                        {
                            Changes.Add(new KeyValuePair<string, object>(kvp.Key, trimmed));
                        }
                    }
                }
            }

            row.MergeWith(Changes);
            yield return row;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class TrimStringMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceNullWithValue(this IFluentProcessMutatorBuilder builder, TrimStringMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}