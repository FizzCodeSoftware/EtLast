namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class OperationGroup : AbstractRowOperation, IOperationGroup
    {
        public IfDelegate If { get; set; }
        public List<IRowOperation> Then { get; } = new List<IRowOperation>();
        public List<IRowOperation> Else { get; } = new List<IRowOperation>();

        public override void Apply(IRow row)
        {
            Stat.IncrementCounter("processed", 1);

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    foreach (var operation in Then)
                    {
                        operation.Apply(row);
                    }

                    Stat.IncrementCounter("then executed", 1);
                }
                else
                {
                    if (Else.Count > 0)
                    {
                        foreach (var operation in Else)
                        {
                            operation.Apply(row);
                        }

                        Stat.IncrementCounter("else executed", 1);
                    }
                }
            }
            else
            {
                foreach (var operation in Then)
                {
                    operation.Apply(row);
                }

                Stat.IncrementCounter("then executed", 1);
            }
        }

        public void AddThenOperation(IRowOperation operation)
        {
            if (Process == null)
                throw new InvalidOperationParameterException(this, nameof(Then), Then, "cannot call " + nameof(AddThenOperation) + " before " + nameof(OperationGroup) + " is added to the parent process");

            operation.SetParentGroup(Process, this, Then.Count);
            Then.Add(operation);
        }

        public void AddElseOperation(IRowOperation operation)
        {
            if (Process == null)
                throw new InvalidOperationParameterException(this, nameof(Then), Then, "cannot call " + nameof(AddElseOperation) + " before " + nameof(OperationGroup) + " is added to the parent process");

            operation.SetParentGroup(Process, this, Else.Count);
            Else.Add(operation);
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