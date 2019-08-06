namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public class SequentialMergeProcess : AbstractMergeProcess
    {
        public SequentialMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
            : base(context, merger, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            Context.Log(LogSeverity.Information, this, "started over {InputCount} processes", InputProcesses.Count);
            var startedOn = Stopwatch.StartNew();

            var resultSets = new List<IEnumerable<IRow>>();
            foreach (var inputProcess in InputProcesses)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return Enumerable.Empty<IRow>();

                resultSets.Add(inputProcess.Evaluate(this));
            }

            var result = Merger.Merge(resultSets);

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", startedOn.Elapsed);

            return result;
        }
    }
}