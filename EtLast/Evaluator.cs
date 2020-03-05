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

        public IEnumerable<IRow> TakeRowsAndTransferOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _process.InvocationInfo?.Caller);
                yield return row;
            }

            if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);
        }

        public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _process.InvocationInfo?.Caller);
                row.Context.SetRowOwner(row, null);

                yield return row;
            }
        }

        public int CountRows()
        {
            var count = 0;
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _process.InvocationInfo?.Caller);
                row.Context.SetRowOwner(row, null);

                count++;
            }

            return count;
        }

        public int CountRowsWithoutTransfer()
        {
            var count = 0;
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, null);
                count++;
            }

            return count;
        }
    }
}