namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ReplaceErrorWithValueMutator : AbstractMutator
    {
        public string[] Columns { get; set; }
        public object Value { get; set; }

        public ReplaceErrorWithValueMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    if (row[column] is EtlRowError)
                    {
                        row.SetStagedValue(column, Value);
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    if (kvp.Value is EtlRowError)
                    {
                        row.SetStagedValue(kvp.Key, Value);
                    }
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ReplaceErrorWithValueMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceErrorWithValue(this IFluentProcessMutatorBuilder builder, ReplaceErrorWithValueMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}