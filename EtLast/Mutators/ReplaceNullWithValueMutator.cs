namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class ReplaceNullWithValueMutator : AbstractMutator
    {
        public string[] Columns { get; set; }
        public object Value { get; set; }

        public ReplaceNullWithValueMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();
            foreach (var column in Columns)
            {
                if (row.IsNull(column))
                {
                    row.SetStagedValue(column, Value);
                }
            }

            row.ApplyStaging(this);

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Value == null)
                throw new ProcessParameterNullException(this, nameof(Value));
        }
    }
}