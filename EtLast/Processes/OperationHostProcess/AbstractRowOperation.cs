namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    [DebuggerDisplay("{" + nameof(Name) + "}")]
    public abstract class AbstractRowOperation : IRowOperation
    {
        public IOperationHostProcess Process { get; private set; }
        IProcess IOperation.Process => Process;

        public int UID { get; private set; }
        public string InstanceName { get; set; }
        public string Name { get; private set; }
        public int Number { get; private set; }

        public IRowOperation NextOperation { get; private set; }
        public IRowOperation PrevOperation { get; private set; }

        public StatCounterCollection CounterCollection { get; private set; }

        private int _hash;

        protected AbstractRowOperation()
        {
            Name = "??." + GetType().GetFriendlyTypeName();
            _hash = Name.GetHashCode(StringComparison.InvariantCultureIgnoreCase);
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

        public virtual void SetNumber(int number)
        {
            Number = number;
            Name = Number.ToString("D2", CultureInfo.InvariantCulture) + "." + (InstanceName ?? GetType().GetFriendlyTypeName());
            _hash = Name.GetHashCode(StringComparison.InvariantCultureIgnoreCase);
        }

        public override int GetHashCode()
        {
            return _hash;
        }

        public abstract void Apply(IRow row);

        public virtual void Prepare()
        {
            UID = Process.Context.GetOperationUid(this);
            PrepareImpl();
        }

        protected abstract void PrepareImpl();

        public virtual void Shutdown()
        {
        }
    }
}