namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ReplaceEmptyStringWithNullMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public ReplaceEmptyStringWithNullMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
        {
            var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();
            foreach (var column in Columns)
            {
#pragma warning disable CA1820 // Test for empty strings using string length
                if (string.Equals(row.GetAs<string>(column, null), string.Empty, StringComparison.InvariantCultureIgnoreCase))
#pragma warning restore CA1820 // Test for empty strings using string length
                {
                    row.SetStagedValue(column, null);
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }
}