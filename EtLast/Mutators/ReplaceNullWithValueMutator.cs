namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ReplaceNullWithValueMutator : AbstractMutator
    {
        public string[] Columns { get; init; }
        public object Value { get; init; }

        public ReplaceNullWithValueMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            foreach (var column in Columns)
            {
                if (!row.HasValue(column))
                {
                    row.SetStagedValue(column, Value);
                }
            }

            row.ApplyStaging();

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