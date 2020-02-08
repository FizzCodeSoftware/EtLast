namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public abstract class AbstractExecutableProcess : AbstractProcess, IExecutable
    {
        protected AbstractExecutableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public void Execute(IProcess caller = null)
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
                return;

            try
            {
                ExecuteImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
        }

        protected abstract void ExecuteImpl();
        protected abstract void ValidateImpl();
    }
}