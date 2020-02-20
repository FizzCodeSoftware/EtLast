namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class RemoveColumnsMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public RemoveColumnsMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            foreach (var column in Columns)
            {
                row.SetStagedValue(column, null);
            }

            row.ApplyStaging();

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Columns == null || Columns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }
    }
}