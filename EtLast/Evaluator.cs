namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class Evaluator
    {
        private readonly IEnumerable<IRow> _input;
        private readonly IProcess _caller;

        public Evaluator()
        {
            _input = Enumerable.Empty<IRow>();
        }

        // caller is passed because process.InvocationInfo.Caller can not be used for this (it stores only the last caller)
        public Evaluator(IProcess caller, IEnumerable<IRow> input)
        {
            _caller = caller;
            _input = input;
        }

        public IEnumerable<IRow> TakeRowsAndTransferOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _caller);
                yield return row;
            }

            /*if (_process != null)
                _process.Context.RegisterProcessInvocationEnd(_process);*/
        }

        public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership()
        {
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _caller);
                row.Context.SetRowOwner(row, null);

                yield return row;
            }
        }

        public int CountRows()
        {
            var count = 0;
            foreach (var row in _input)
            {
                row.Context.SetRowOwner(row, _caller);
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