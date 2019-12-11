namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IOperationGroup : IRowOperation
    {
        RowTestDelegate If { get; set; }

        List<IRowOperation> Then { get; }
        List<IRowOperation> Else { get; }

        void AddThenOperation(IRowOperation operation);
        void AddElseOperation(IRowOperation operation);
    }
}