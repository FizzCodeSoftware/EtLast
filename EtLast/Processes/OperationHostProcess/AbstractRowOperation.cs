namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public delegate void CustomPrepareDelegate(IRowOperation op);

    [DebuggerDisplay("{" + nameof(Name) + "}")]
    public abstract class AbstractRowOperation : IRowOperation
    {
        public IOperationHostProcess Process { get; private set; }
        IProcess IOperation.Process => Process;

        public int UID { get; private set; }
        private string _instanceName;

        public string InstanceName
        {
            get => _instanceName; set
            {
                _instanceName = value;
                Name = value;
            }
        }

        public string Name { get; private set; }

        public IRowOperation NextOperation { get; private set; }
        public IRowOperation PrevOperation { get; private set; }

        public StatCounterCollection CounterCollection { get; private set; }

        public CustomPrepareDelegate OnCustomPrepare { get; set; }

        protected AbstractRowOperation()
        {
            Name = GetType().GetFriendlyTypeName();
        }

        public void SetNextOperation(IRowOperation operation)
        {
            NextOperation = operation;
        }

        public void SetPrevOperation(IRowOperation operation)
        {
            PrevOperation = operation;
        }

        public virtual void SetProcess(IOperationHostProcess process)
        {
            Process = process;
            CounterCollection = process != null
                ? new StatCounterCollection(process.CounterCollection)
                : null;
        }

        public void SetProcess(IProcess process)
        {
            if (process == null)
            {
                SetProcess(null);
                return;
            }

            if (!(process is IOperationHostProcess operationProcess))
                throw new InvalidOperationParameterException(this, "parent process", process, "parent process must be an IOperationHostProcess");

            SetProcess(operationProcess);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode(StringComparison.InvariantCultureIgnoreCase);
        }

        public abstract void Apply(IRow row);

        public virtual void Prepare()
        {
            UID = Process.Context.GetOperationUid(this);
            PrepareImpl();

            OnCustomPrepare?.Invoke(this);
        }

        protected abstract void PrepareImpl();

        public virtual void Shutdown()
        {
        }
    }
}