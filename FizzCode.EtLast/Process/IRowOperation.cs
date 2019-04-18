namespace FizzCode.EtLast
{
    public interface IRowOperation : IBaseOperation
    {
        IOperationGroup ParentGroup { get; }
        IRowOperation NextOperation { get; }
        IRowOperation PrevOperation { get; }
        OperationStat Stat { get; }

        new IOperationProcess Process { get; }

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        void SetParent(IOperationProcess process, int index);

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        void SetParentGroup(IOperationProcess process, IOperationGroup parentGroup, int index);

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        void SetNextOperation(IRowOperation operation);

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        void SetPrevOperation(IRowOperation operation);

        void Apply(IRow row);
    }
}