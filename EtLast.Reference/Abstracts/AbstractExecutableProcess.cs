namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public abstract class AbstractExecutableProcess : AbstractProcess, IExecutable
    {
        protected AbstractExecutableProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public void Execute(IProcess caller = null)
        {
            Caller = caller;
            Validate();

            try
            {
                var startedOn = Stopwatch.StartNew();
                Execute(startedOn);
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
        }

        protected abstract void Execute(Stopwatch startedOn);
    }
}