namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class SequentialMergeProcess : AbstractMergeProcess
    {
        public SequentialMergeProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "started");

            foreach (var inputProcess in ProcessList)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                var rows = inputProcess.Evaluate(this);
                foreach (var row in rows)
                {
                    Context.SetRowOwner(row, this);

                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);
        }
    }
}