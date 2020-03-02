namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public abstract class AbstractExecutable : AbstractProcess, IExecutable
    {
        protected AbstractExecutable(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public void Execute(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                ValidateImpl();

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return;

                ExecuteImpl();
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
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