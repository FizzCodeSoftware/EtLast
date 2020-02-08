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

        protected AbstractEvaluableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public Evaluator Evaluate(IProcess caller = null)
        {
            Context.GetProcessUid(this);

            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            try
            {
                ValidateImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

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
                return new Evaluator(EvaluateImpl());
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return new Evaluator();
        }

        protected abstract IEnumerable<IRow> EvaluateImpl();
        protected abstract void ValidateImpl();

        public void Execute(IProcess caller)
        {
            var evaluator = Evaluate();
            _ = evaluator.CountRows(null);
        }
    }
}