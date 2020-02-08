namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public delegate bool CustomMutatorDelegate(IEvaluable process, IRow row);

    public class CustomMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public CustomMutatorDelegate Then { get; set; }
        public CustomMutatorDelegate Else { get; set; }

        public CustomMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                var keep = true;

                try
                {
                    if (If != null)
                    {
                        var result = If.Invoke(row);
                        if (result)
                        {
                            keep = Then.Invoke(this, row);
                            CounterCollection.IncrementCounter("then executed", 1);
                        }
                        else if (Else != null)
                        {
                            keep = Else.Invoke(this, row);
                            CounterCollection.IncrementCounter("else executed", 1);
                        }
                    }
                    else
                    {
                        keep = Then.Invoke(this, row);
                        CounterCollection.IncrementCounter("then executed", 1);
                    }
                }
                catch (ProcessExecutionException) { throw; }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, row, ex);
                    Context.AddException(this, exception);
                    keep = false;
                }

                if (keep)
                    yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (Then == null)
                throw new ProcessParameterNullException(this, nameof(Then));

            if (Else != null && If == null)
                throw new ProcessParameterNullException(this, nameof(If));
        }
    }
}