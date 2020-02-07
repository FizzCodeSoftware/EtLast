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

        public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess newOwner)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, newOwner);
                yield return row;
            }
        }

        public IEnumerable<IRow> TakeRowsAndReleaseOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, null);
                yield return row;
            }
        }

        public IEnumerable<IRow> TakeRowsAndReleaseOwnership(IProcess process)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, process, null);
                row.Context.SetRowOwner(row, null, null);

                yield return row;
            }
        }

        public IEnumerable<IRow> TakeRowsAndReleaseOwnership(IOperation operation)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, operation.Process, operation);
                row.Context.SetRowOwner(row, null, operation);

                yield return row;
            }
        }

        public int CountRows(IProcess newOwner, IOperation operation = null)
        {
            var count = 0;
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, newOwner);

                if (newOwner != null)
                    row.Context.SetRowOwner(row, null, operation);

                count++;
            }

            return count;
        }
    }
}