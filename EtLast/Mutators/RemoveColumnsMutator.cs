namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RemoveColumnsMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public RemoveColumnsMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            foreach (var column in Columns)
            {
                row.SetValue(column, null, this);
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Columns == null || Columns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }
    }
}