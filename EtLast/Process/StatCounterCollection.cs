namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class StatCounterCollection
    {
        public static string DebugNamePrefix { get; } = "debug-";

        public IDictionary<string, long> GetCountersOrdered()
        {
            lock (_counters)
            {
                var result = new Dictionary<string, long>();
                foreach (var kvp in _counters.OrderBy(x => x.Key))
                {
                    result.Add(kvp.Key, kvp.Value);
                }

                return result;
            }
        }

        private readonly Dictionary<string, long> _counters = new Dictionary<string, long>();

        public void IncrementCounter(string name, long n)
        {
            lock (_counters)
            {
                _counters.TryGetValue(name, out var value);
                _counters[name] = value += n;
            }
        }

        public void IncrementDebugCounter(string name, long n)
        {
            name = DebugNamePrefix + name;

            lock (_counters)
            {
                _counters.TryGetValue(name, out var value);
                _counters[name] = value += n;
            }
        }

        public void Clear()
        {
            lock (_counters)
            {
                _counters.Clear();
            }
        }
    }
}