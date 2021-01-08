namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class TrimStringMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public TrimStringMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    if (row[column] is string str && !string.IsNullOrEmpty(str))
                    {
                        var trimmed = str.Trim();
                        if (trimmed != str)
                        {
                            row.SetStagedValue(column, trimmed);
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
                            row.SetStagedValue(kvp.Key, trimmed);
                        }
                    }
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class TrimStringMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceNullWithValue(this IFluentProcessMutatorBuilder builder, TrimStringMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}