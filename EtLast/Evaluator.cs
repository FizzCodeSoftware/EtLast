namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class Evaluator
    {
        private readonly IEnumerable<IRow> _input;
        private readonly IProcess _process;

        public Evaluator()
        {
            _input = Enumerable.Empty<IRow>();
        }

        public Evaluator(IProcess process, IEnumerable<IRow> input)
        {
            _process = process;
            _input = input;
        }

        public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess newOwner)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, newOwner);
                yield return row;
            }

            if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);
        }

        public IEnumerable<IRow> TakeRowsAndReleaseOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, null);
                yield return row;
            }

            if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);
        }

        public IEnumerable<IRow> TakeRowsAndReleaseOwnership(IProcess process)
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, process);
                row.Context.SetRowOwner(row, null);

                yield return row;
            }

            if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);
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

            if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);

            return count;
        }
    }
}