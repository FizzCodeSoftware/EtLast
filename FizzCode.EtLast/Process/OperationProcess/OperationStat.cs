namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class OperationStat
    {
        public IDictionary<string, long> Counters
        {
            get
            {
                lock (_counters)
                {
                    var result = new Dictionary<string, long>();
                    foreach (var kvp in _counters)
                    {
                        result.Add(kvp.Key, kvp.Value);
                    }

                    return result;
                }
            }
        }

        private readonly Dictionary<string, long> _counters = new Dictionary<string, long>();

        public void IncrementCounter(string name, long n)
        {
            lock (_counters)
            {
                _counters.TryGetValue(name, out long value);
                _counters[name] = value += n;
            }
        }
    }
}