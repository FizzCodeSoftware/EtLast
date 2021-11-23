namespace FizzCode.EtLast
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractExecutableWithResult<T> : AbstractProcess, IExecutableWithResult<T>
    {
        protected AbstractExecutableWithResult(IEtlContext context)
            : base(context)
        {
        }

        public T Execute(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                ValidateImpl();

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return default;

                return ExecuteImpl();
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

            return default;
        }

        protected abstract void ValidateImpl();
        protected abstract T ExecuteImpl();
    }
}