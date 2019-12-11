namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    [DebuggerDisplay("{" + nameof(Name) + "}")]
    public abstract class AbstractRowOperation : IRowOperation
    {
        public IOperationHostProcess Process { get; private set; }
        IProcess IBaseOperation.Process => Process;

        public string InstanceName { get; set; }
        public string Name { get; private set; }
        public int Number { get; private set; }

        public IRowOperation NextOperation { get; private set; }
        public IRowOperation PrevOperation { get; private set; }

        public StatCounterCollection CounterCollection { get; private set; }

        private int _hash;

        protected AbstractRowOperation()
        {
            Name = "??." + TypeHelpers.GetFriendlyTypeName(GetType());
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
            CounterCollection = new StatCounterCollection(process.CounterCollection);
        }

        public void SetProcess(IProcess process)
        {
            if (!(process is IOperationHostProcess operationProcess))
                throw new InvalidOperationParameterException(this, "parent process", process, "parent process must be an IOperationHostProcess");

            SetProcess(operationProcess);
        }

        public virtual void SetNumber(int number)
        {
            Number = number;
            Name = Number.ToString("D2", CultureInfo.InvariantCulture) + "." + (InstanceName ?? TypeHelpers.GetFriendlyTypeName(GetType()));
            _hash = Name.GetHashCode(StringComparison.InvariantCultureIgnoreCase);
        }

        public override int GetHashCode()
        {
            return _hash;
        }

        public abstract void Apply(IRow row);

        public virtual void Prepare()
        {
        }

        public virtual void Shutdown()
        {
        }
    }
}