﻿namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public abstract class AbstractProcess : IProcess
    {
        public int UID { get; }
        public ProcessTestDelegate If { get; set; }
        public IEtlContext Context { get; }
        public IProcess Caller { get; protected set; }
        public string Name { get; set; }
        public string Topic { get; set; }
        public Stopwatch LastInvocation { get; protected set; }

        public StatCounterCollection CounterCollection { get; }

        protected AbstractProcess(IEtlContext context, string name, string topic)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name ?? GetType().GetFriendlyTypeName();
            Topic = topic;
            CounterCollection = new StatCounterCollection(context.CounterCollection);

            UID = Context.GetProcessUid(this);
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

            Context.Log(LogSeverity.Debug, this, "PROCESS COUNTERS");
            foreach (var counter in counters)
            {
                Context.Log(LogSeverity.Debug, this, "{Counter} = {Value}", counter.Name, counter.TypedValue);
            }
        }
    }
}