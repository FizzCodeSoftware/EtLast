namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

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
            var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();
            foreach (var column in Columns)
            {
                if (row[column] is EtlRowError)
                {
                    row.SetStagedValue(column, Value);
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }
}