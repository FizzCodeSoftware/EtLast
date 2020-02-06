namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class Evaluator
    {
        private readonly IEnumerable<IRow> _input;

        public Evaluator()
        {
            _input = Enumerable.Empty<IRow>();
        }

        public Evaluator(IEnumerable<IRow> input)
        {
            _input = input;
        }

        public IEnumerable<IRow> TakeRows(IProcess newOwner, bool immediatelyReleaseOwnership = false)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, newOwner);
                if (immediatelyReleaseOwnership && newOwner != null)
                    row.Context.SetRowOwner(row, null);

                yield return row;
            }
        }

        public int CountRows(IProcess newOwner)
        {
            var count = 0;
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, newOwner);

                if (newOwner != null)
                    row.Context.SetRowOwner(row, null);

                count++;
            }

            return count;
        }
    }
}