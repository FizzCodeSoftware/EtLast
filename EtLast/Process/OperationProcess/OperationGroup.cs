namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class OperationGroup : AbstractRowOperation, IOperationGroup
    {
        public RowTestDelegate If { get; set; }
        public List<IRowOperation> Then { get; } = new List<IRowOperation>();
        public List<IRowOperation> Else { get; } = new List<IRowOperation>();

        public override void Apply(IRow row)
        {
            Stat.IncrementDebugCounter("executed", 1);

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    foreach (var operation in Then)
                    {
                        operation.Apply(row);
                    }

                    Stat.IncrementDebugCounter("then executed", 1);
                }
                else if (Else.Count > 0)
                {
                    foreach (var operation in Else)
                    {
                        operation.Apply(row);
                    }

                    Stat.IncrementDebugCounter("else executed", 1);
                }
            }
            else
            {
                foreach (var operation in Then)
                {
                    operation.Apply(row);
                }

                Stat.IncrementDebugCounter("then executed", 1);
            }
        }

        public void AddThenOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            operation.SetParentGroup(this, Then.Count);
            Then.Add(operation);
        }

        public void AddElseOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            operation.SetParentGroup(this, Else.Count);
            Else.Add(operation);
        }

        public override void SetProcess(IOperationProcess process)
        {
            base.SetProcess(process);

            foreach (var op in Then)
            {
                op.SetProcess(Process);
            }

            foreach (var op in Else)
            {
                op.SetProcess(Process);
            }
        }

        public override void SetParent(int index)
        {
            base.SetParent(index);

            var idx = 0;
            foreach (var op in Then)
            {
                op.SetParentGroup(this, idx);
                idx++;
            }

            idx = 0;
            foreach (var op in Else)
            {
                op.SetParentGroup(this, idx);
                idx++;
            }
        }

        public override void Prepare()
        {
            if (Then.Count == 0)
                throw new OperationParameterNullException(this, nameof(Then));
            if (Else.Count > 0 && If == null)
                throw new OperationParameterNullException(this, nameof(If));
        }
    }
}