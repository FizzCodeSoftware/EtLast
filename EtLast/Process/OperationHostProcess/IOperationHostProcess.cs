namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IOperationHostProcess : IEvaluable, IExecutable
    {
        IEvaluable InputProcess { get; set; }
        OperationHostProcessConfiguration Configuration { get; }
        bool ReadingInput { get; }

        List<IRowOperation> Operations { get; set; }
        T AddOperation<T>(T operation) where T : IRowOperation;

        void AddRow(IRow row, IRowOperation operation);
        void AddRows(ICollection<IRow> rows, IRowOperation operation);

        void RemoveRow(IRow row, IRowOperation operation);
        void RemoveRows(IEnumerable<IRow> rows, IRowOperation operation);
    }
}