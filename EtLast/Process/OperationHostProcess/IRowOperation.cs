namespace FizzCode.EtLast
{
    using System.ComponentModel;

    public interface IRowOperation : IBaseOperation
    {
        IRowOperation NextOperation { get; }
        IRowOperation PrevOperation { get; }
        StatCounterCollection Stat { get; }

        new IOperationHostProcess Process { get; }

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetProcess(IOperationHostProcess process);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetNextOperation(IRowOperation operation);

        [EditorBrowsable(EditorBrowsableState.Never)]
        void SetPrevOperation(IRowOperation operation);

        void Apply(IRow row);
    }
}