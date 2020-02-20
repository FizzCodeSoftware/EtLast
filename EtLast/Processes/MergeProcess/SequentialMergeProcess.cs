namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class SequentialMergeProcess : AbstractMergeProcess
    {
        public SequentialMergeProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
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

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocationStarted.Elapsed);
        }
    }
}