namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractEvaluableProcess : AbstractProcess, IEvaluable
    {
        public virtual bool ConsumerShouldNotBuffer { get; }

        protected AbstractEvaluableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public Evaluator Evaluate(IProcess caller = null)
        {
            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            Validate();

            if (Context.CancellationTokenSource.IsCancellationRequested)
                return new Evaluator();

            if (If?.Invoke(this) == false)
                return new Evaluator();

            try
            {
                return new Evaluator(EvaluateImpl());
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return new Evaluator();
        }

        protected abstract IEnumerable<IRow> EvaluateImpl();
    }
}