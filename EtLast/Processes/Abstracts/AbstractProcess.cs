using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
            var sb = new StringBuilder();
            var parameters = new List<object>();

            var counters = CounterCollection
                .GetCounters()
                .Where(x => !x.IsDebug)
                .ToList();

            if (counters.Count > 0)
            {
                sb.Append("counters");

                foreach (var counter in counters)
                {
                    sb.Append(" [")
                        .Append("{Counter").Append(counter.Code).Append('}')
                        .Append(" = {Value").Append(counter.Code).Append("}]");

                    parameters.Add(counter.Name);
                    parameters.Add(counter.Value);
                }

                Context.Log(LogSeverity.Information, this, sb.ToString(), parameters.ToArray());
            }

            counters = CounterCollection
                .GetCounters()
                .Where(x => x.IsDebug)
                .ToList();

            if (counters.Count > 0)
            {
                parameters.Clear();
                sb.Clear();

                sb.Append("counters (debug)");

                foreach (var counter in counters)
                {
                    sb.Append(" [")
                        .Append("{Counter").Append(counter.Code).Append('}')
                        .Append(" = {Value").Append(counter.Code).Append("}]");

                    parameters.Add(counter.Name);
                    parameters.Add(counter.Value);
                }

                Context.Log(LogSeverity.Debug, this, sb.ToString(), parameters.ToArray());
            }
        }
    }
}