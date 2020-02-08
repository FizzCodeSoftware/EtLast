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
            CounterCollection.IncrementCounter("executed", 1);

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    foreach (var operation in Then)
                    {
                        operation.Apply(row);
                    }

                    CounterCollection.IncrementCounter("then executed", 1);
                }
                else if (Else.Count > 0)
                {
                    foreach (var operation in Else)
                    {
                        operation.Apply(row);
                    }

                    CounterCollection.IncrementCounter("else executed", 1);
                }
            }
            else
            {
                foreach (var operation in Then)
                {
                    operation.Apply(row);
                }

                CounterCollection.IncrementCounter("then executed", 1);
            }
        }

        public void AddThenOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            Then.Add(operation);
        }

        public void AddElseOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            Else.Add(operation);
        }

        public override void SetProcess(IOperationHostProcess process)
        {
            base.SetProcess(process);

            foreach (var op in Then)
            {
                op.SetProcess(process);
            }

            foreach (var op in Else)
            {
                op.SetProcess(process);
            }
        }

        protected override void PrepareImpl()
        {
            if (Then.Count == 0)
                throw new OperationParameterNullException(this, nameof(Then));

            if (Else.Count > 0 && If == null)
                throw new OperationParameterNullException(this, nameof(If));
        }
    }
}