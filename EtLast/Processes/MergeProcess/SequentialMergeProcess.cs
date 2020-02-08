namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class SequentialMergeProcess : AbstractMergeProcess
    {
        public SequentialMergeProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "started");

            foreach (var inputProcess in ProcessList)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                var rows = inputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);
                foreach (var row in rows)
                {
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);
        }
    }
}