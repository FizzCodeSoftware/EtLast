using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;

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

            var sb = new StringBuilder();
            var parameters = new List<object>();

            foreach (var counter in counters)
            {
                sb.Append("counter {Counter} = {Value}");
                parameters.Add(counter.Name);
                parameters.Add(counter.Value.TypedValue);
                if (counter.SubValues != null)
                {
                    var idx = 0;
                    foreach (var kvp in counter.SubValues)
                    {
                        sb
                            .Append(", {Sub")
                            .Append(idx.ToString("D", CultureInfo.InvariantCulture))
                            .Append("} = {SubValue")
                            .Append(idx.ToString("D", CultureInfo.InvariantCulture))
                            .Append('}');

                        parameters.Add(kvp.Key);
                        parameters.Add(kvp.Value.TypedValue);
                        idx++;
                    }
                }

                Context.Log(counter.IsDebug ? LogSeverity.Debug : LogSeverity.Information, this, sb.ToString(), parameters.ToArray());
                sb.Clear();
                parameters.Clear();
            }
        }
    }
}