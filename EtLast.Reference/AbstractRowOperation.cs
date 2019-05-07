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

        public void SetParent(IOperationProcess process, int index)
        {
            Process = process;
            ParentGroup = null;
            Index = index;
            Name = Index.ToString("D2") + "." + (InstanceName != null ? InstanceName + "(" + GetType().Name + ")" : GetType().Name);
            _hash = Name.GetHashCode();
        }

        public void SetParent(IProcess process, int index)
        {
            if (!(process is IOperationProcess pr)) throw new InvalidOperationParameterException(this, "parent process", process, "parent process must be an IOperationProcess");
            SetParent(pr, index);
        }

        public void SetParentGroup(IOperationProcess process, IOperationGroup parentGroup, int index)
        {
            Process = process;
            ParentGroup = parentGroup;
            Index = index;
            Name = (ParentGroup != null ? ParentGroup.Name + "|" : "") + Index.ToString("D2") + "." + (InstanceName != null ? InstanceName + "(" + GetType().Name + ")" : GetType().Name);
            _hash = Name.GetHashCode();
        }

        public override int GetHashCode()
        {
            return _hash;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public abstract void Apply(IRow row);

        public abstract void Prepare();

        public virtual void Shutdown()
        {
        }
    }
}