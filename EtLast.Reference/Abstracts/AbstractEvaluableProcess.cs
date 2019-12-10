namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public abstract class AbstractEvaluableProcess : AbstractProcess, IEvaluable
    {
        public virtual bool ConsumerShouldNotBuffer { get; }

        protected AbstractEvaluableProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            Validate();

            if (Context.CancellationTokenSource.IsCancellationRequested)
                return Enumerable.Empty<IRow>();

            if (If?.Invoke(this) == false)
                return Enumerable.Empty<IRow>();

            try
            {
                return EvaluateImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return Enumerable.Empty<IRow>();
        }

        protected abstract IEnumerable<IRow> EvaluateImpl();
    }
}