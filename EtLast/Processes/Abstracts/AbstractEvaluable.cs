namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractEvaluable : AbstractProcess, IEvaluable
    {
        public virtual bool ConsumerShouldNotBuffer { get; }
        public Action<IEvaluable> Initializer { get; init; }

        protected AbstractEvaluable(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public Evaluator Evaluate(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                ValidateImpl();

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return new Evaluator();

                if (Initializer != null)
                {
                    try
                    {
                        Initializer.Invoke(this);
                    }
                    catch (Exception ex)
                    {
                        throw new ProcessExecutionException(this, "error during the initialization of the process", ex);
                    }

                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        return new Evaluator();
                }

                return new Evaluator(this, caller, EvaluateImpl(netTimeStopwatch));
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
            }

            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            return new Evaluator();
        }

        protected abstract IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch);
        protected abstract void ValidateImpl();

        public void Execute(IProcess caller)
        {
            var evaluator = Evaluate(caller);
            _ = evaluator.CountRowsWithoutTransfer();
        }
    }
}