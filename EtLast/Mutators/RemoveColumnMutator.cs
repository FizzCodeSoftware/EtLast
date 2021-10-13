namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class RemoveColumnMutator : AbstractMutator
    {
        public string[] Columns { get; init; }

        public RemoveColumnMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns.Length > 1)
            {
                foreach (var column in Columns)
                {
                    row.SetStagedValue(column, null);
                }

                row.ApplyStaging();
            }
            else
            {
                row.SetValue(Columns[0], null);
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Columns == null || Columns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class RemoveColumnMutatorFluent
    {
        public static IFluentProcessMutatorBuilder RemoveColumn(this IFluentProcessMutatorBuilder builder, RemoveColumnMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}