using System;
using System.Diagnostics;

namespace FizzCode.EtLast
{
    public abstract class AbstractProcess : IProcess
    {
        public ProcessTestDelegate If { get; set; }
        public IEtlContext Context { get; }
        public IProcess Caller { get; protected set; }
        public string Name { get; }
        public Stopwatch LastInvocation { get; protected set; }

        public StatCounterCollection CounterCollection { get; }

        protected AbstractProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name ?? TypeHelpers.GetFriendlyTypeName(GetType());
            CounterCollection = new StatCounterCollection(context.CounterCollection);
        }

        public void Validate()
        {
            try
            {
                ValidateImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
        }

        public abstract void ValidateImpl();

        protected void LogCounters()
        {
            var counters = CounterCollection.GetCounters();
            if (counters.Count == 0)
                return;

            foreach (var counter in counters)
            {
                Context.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, this, "counter {Counter} = {Value}", counter.Name, counter.TypedValue);
            }
        }
    }
}