namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ReplaceNullWithValueMutator : AbstractSimpleChangeMutator
    {
        public string[] Columns { get; init; }
        public object Value { get; init; }

        public ReplaceNullWithValueMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns.Length > 1)
            {
                Changes.Clear();
                foreach (var column in Columns)
                {
                    if (!row.HasValue(column))
                    {
                        Changes.Add(new KeyValuePair<string, object>(column, Value));
                    }
                }

                row.MergeWith(Changes);
            }
            else if (!row.HasValue(Columns[0]))
            {
                row[Columns[0]] = Value;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Value == null)
                throw new ProcessParameterNullException(this, nameof(Value));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ReplaceNullWithValueMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceNullWithValue(this IFluentProcessMutatorBuilder builder, ReplaceNullWithValueMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}