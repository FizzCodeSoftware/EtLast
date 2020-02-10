namespace FizzCode.EtLast
{
    using System;

    public abstract class AbstractExecutableProcess : AbstractProcess, IExecutable
    {
        protected AbstractExecutableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public void Execute(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

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

            Context.RegisterProcessInvocationEnd(this);
        }

        protected abstract void ExecuteImpl();
        protected abstract void ValidateImpl();
    }
}