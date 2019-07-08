using System.ComponentModel;

namespace FizzCode.EtLast
{
    public interface IRowOperation : IBaseOperation
    {
        IOperationGroup ParentGroup { get; }
        IRowOperation NextOperation { get; }
        IRowOperation PrevOperation { get; }
        StatCounterCollection Stat { get; }

        new IOperationProcess Process { get; }

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetProcess(IOperationProcess process);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetParentGroup(IOperationGroup parentGroup, int index);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetNextOperation(IRowOperation operation);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetPrevOperation(IRowOperation operation);

        void Apply(IRow row);
    }
}