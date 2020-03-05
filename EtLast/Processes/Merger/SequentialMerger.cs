namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;

    public class SequentialMerger : AbstractMerger
    {
        public SequentialMerger(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IEtlRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            foreach (var inputProcess in ProcessList)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                var rows = inputProcess.Evaluate(this).TakeRowsAndTransferOwnership();
                foreach (var row in rows)
                {
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
            Context.RegisterProcessInvocationEnd(this);
        }
    }
}