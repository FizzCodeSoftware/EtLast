namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public delegate void EvaluableInitializerDelegate(IEvaluable evaluable);

    public abstract class AbstractEvaluableProcess : AbstractProcess, IEvaluable
    {
        public virtual bool ConsumerShouldNotBuffer { get; }
        public EvaluableInitializerDelegate Initializer { get; set; }

        protected AbstractEvaluableProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public Evaluator Evaluate(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                try
                {
                    ValidateImpl();
                }
                catch (EtlException ex)
                {
                    Context.AddException(this, ex);
                    return new Evaluator();
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, ex));
                    return new Evaluator();
                }

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return new Evaluator();

                if (Initializer != null)
                {
                    Initializer.Invoke(this);

                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        return new Evaluator();
                }

                try
                {
                    return new Evaluator(this, EvaluateImpl(netTimeStopwatch));
                }
                catch (EtlException ex) { Context.AddException(this, ex); }
                catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
            }
            finally
            {
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            }

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