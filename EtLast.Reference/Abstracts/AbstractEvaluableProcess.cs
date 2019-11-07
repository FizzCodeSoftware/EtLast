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
            Caller = caller;
            Validate();

            try
            {
                var startedOn = Stopwatch.StartNew();

                var rows = Evaluate(startedOn);
                return rows;
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return Enumerable.Empty<IRow>();
        }

        protected abstract IEnumerable<IRow> Evaluate(Stopwatch startedOn);
    }
}