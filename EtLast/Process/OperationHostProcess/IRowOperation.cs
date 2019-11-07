namespace FizzCode.EtLast
{
    using System.ComponentModel;

    public interface IRowOperation : IBaseOperation
    {
        IOperationGroup ParentGroup { get; }
        IRowOperation NextOperation { get; }
        IRowOperation PrevOperation { get; }
        StatCounterCollection Stat { get; }

        new IOperationHostProcess Process { get; }

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetProcess(IOperationHostProcess process);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetParentGroup(IOperationGroup parentGroup, int number);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetNextOperation(IRowOperation operation);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetPrevOperation(IRowOperation operation);

        void Apply(IRow row);
    }
}