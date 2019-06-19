namespace FizzCode.EtLast
{
    using System.Diagnostics;

    [DebuggerDisplay("{" + nameof(Name) + "}")]
    public abstract class AbstractRowOperation : IRowOperation
    {
        public IOperationProcess Process { get; private set; }
        public IOperationGroup ParentGroup { get; private set; }
        IProcess IBaseOperation.Process => Process;

        public string InstanceName { get; set; }
        public string Name { get; private set; }
        public int Index { get; private set; }

        public IRowOperation NextOperation { get; private set; }
        public IRowOperation PrevOperation { get; private set; }

        public OperationStat Stat { get; } = new OperationStat();

        private int _hash;

        protected AbstractRowOperation()
        {
            Name = "??." + GetType().Name;
            _hash = Name.GetHashCode();
        }

        public void SetNextOperation(IRowOperation operation)
        {
            NextOperation = operation;
        }

        public void SetPrevOperation(IRowOperation operation)
        {
            PrevOperation = operation;
        }

        public virtual void SetProcess(IOperationProcess process)
        {
            Process = process;
        }

        public void SetProcess(IProcess process)
        {
            if (!(process is IOperationProcess operationProcess))
                throw new InvalidOperationParameterException(this, "parent process", process, "parent process must be an IOperationProcess");

            SetProcess(operationProcess);
        }

        public void SetParent(int index)
        {
            ParentGroup = null;
            Index = index;
            Name = Index.ToString("D2") + "." + (InstanceName != null ? InstanceName + "(" + GetType().Name + ")" : GetType().Name);
            _hash = Name.GetHashCode();
        }

        public void SetParentGroup(IOperationGroup parentGroup, int index)
        {
            ParentGroup = parentGroup;
            Index = index;
            Name = (ParentGroup != null ? ParentGroup.Name + "|" : "") + Index.ToString("D2") + "." + (InstanceName != null ? InstanceName + "(" + GetType().Name + ")" : GetType().Name);
            _hash = Name.GetHashCode();
        }

        public override int GetHashCode()
        {
            return _hash;
        }

        public abstract void Apply(IRow row);

        public abstract void Prepare();

        public virtual void Shutdown()
        {
        }
    }
}