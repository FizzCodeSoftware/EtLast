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
            CounterCollection.IncrementDebugCounter("executed", 1);

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    foreach (var operation in Then)
                    {
                        operation.Apply(row);
                    }

                    CounterCollection.IncrementDebugCounter("then executed", 1);
                }
                else if (Else.Count > 0)
                {
                    foreach (var operation in Else)
                    {
                        operation.Apply(row);
                    }

                    CounterCollection.IncrementDebugCounter("else executed", 1);
                }
            }
            else
            {
                foreach (var operation in Then)
                {
                    operation.Apply(row);
                }

                CounterCollection.IncrementDebugCounter("then executed", 1);
            }
        }

        public void AddThenOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            operation.SetNumber(Then.Count + 1);
            Then.Add(operation);
        }

        public void AddElseOperation(IRowOperation operation)
        {
            if (operation is IDeferredRowOperation)
                throw new InvalidOperationParameterException(this, nameof(operation), null, "deferred operations are not supported in " + nameof(OperationGroup));

            operation.SetNumber(Then.Count + 1);
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

        public override void SetNumber(int number)
        {
            base.SetNumber(number);

            var idx = 1;
            foreach (var op in Then)
            {
                op.SetNumber(idx);
                idx++;
            }

            idx = 1;
            foreach (var op in Else)
            {
                op.SetNumber(idx);
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