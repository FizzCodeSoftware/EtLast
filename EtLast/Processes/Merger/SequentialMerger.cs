namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    public sealed class SequentialMerger : AbstractMerger
    {
        public SequentialMerger(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class SequentialMergerFluent
    {
        public static IFluentProcessMutatorBuilder SequentialMerge(this IFluentProcessBuilder builder, string name, Action<SequentialMergerBuilder> action)
        {
            var subBuilder = new SequentialMergerBuilder(builder.Result.Context, name);
            action.Invoke(subBuilder);
            return builder.ReadFrom(subBuilder.Merger);
        }
    }

    public class SequentialMergerBuilder
    {
        public SequentialMerger Merger { get; }

        internal SequentialMergerBuilder(IEtlContext context, string name)
        {
            Merger = new SequentialMerger(context)
            {
                Name = name,
                ProcessList = new List<IProducer>(),
            };
        }

        public SequentialMergerBuilder AddInput(Action<IFluentProcessBuilder> action)
        {
            var subBuilder = ProcessBuilder.Fluent;
            action.Invoke(subBuilder);
            Merger.ProcessList.Add(subBuilder.Result);
            return this;
        }
    }
}