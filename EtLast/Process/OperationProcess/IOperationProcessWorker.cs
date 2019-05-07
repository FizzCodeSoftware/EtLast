namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Threading;

    public interface IOperationProcessWorker
    {
        void Process(IEnumerable<IRow> rows, AbstractOperationProcess process, CancellationToken token);
    }
}