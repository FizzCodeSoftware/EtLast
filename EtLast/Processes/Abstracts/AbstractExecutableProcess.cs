namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public abstract class AbstractExecutableProcess : AbstractProcess, IExecutable
    {
        protected AbstractExecutableProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public void Execute(IProcess caller = null)
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
                    return;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, ex));
                    return;
                }

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return;

                try
                {
                    ExecuteImpl();
                }
                catch (EtlException ex) { Context.AddException(this, ex); }
                catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
            }
            finally
            {
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            }
        }

        protected abstract void ExecuteImpl();
        protected abstract void ValidateImpl();
    }
}