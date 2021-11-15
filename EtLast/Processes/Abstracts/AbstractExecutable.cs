namespace FizzCode.EtLast
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractExecutable : AbstractProcess, IExecutable
    {
        protected AbstractExecutable(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        public void Execute(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);
            Context.Log(LogSeverity.Information, caller, "executing process {Process}", Name);

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