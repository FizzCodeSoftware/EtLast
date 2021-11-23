namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;

    public sealed class RemoveColumnMutator : AbstractSimpleChangeMutator
    {
        public string[] Columns { get; init; }

        public RemoveColumnMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override void StartMutator()
        {
            base.StartMutator();

            if (Columns.Length > 1)
            {
                Changes.AddRange(Columns.Select(x => new KeyValuePair<string, object>(x, null)));
            }
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (Columns.Length > 1)
            {
                row.MergeWith(Changes);
            }
            else
            {
                row[Columns[0]] = null;
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

        public static IFluentProcessMutatorBuilder RemoveColumn(this IFluentProcessMutatorBuilder builder, params string[] columns)
        {
            return builder.AddMutator(new RemoveColumnMutator(builder.ProcessBuilder.Result.Context)
            {
                Columns = columns,
            });
        }
    }
}