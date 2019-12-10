namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class SequentialMergeProcess : AbstractMergeProcess
    {
        public SequentialMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
            : base(context, merger, name)
        {
        }

        public override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "started");

            var resultSets = new List<IEnumerable<IRow>>();
            foreach (var inputProcess in ProcessList)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return Enumerable.Empty<IRow>();

                resultSets.Add(inputProcess.Evaluate(this));
            }

            var result = Merger.Merge(resultSets);

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);
            return result;
        }
    }
}