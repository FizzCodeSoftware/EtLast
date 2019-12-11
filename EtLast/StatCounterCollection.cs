namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public class StatCounterCollection
    {
        private readonly StatCounterCollection _forwardCountersToCollection;
        private readonly Dictionary<string, StatCounter> _counters = new Dictionary<string, StatCounter>();

        public StatCounterCollection()
        {
        }

        public StatCounterCollection(StatCounterCollection forwardCountersToCollection)
        {
            _forwardCountersToCollection = forwardCountersToCollection;
        }

        public List<StatCounter> GetCounters()
        {
            lock (_counters)
            {
                var result = new List<StatCounter>();
                foreach (var kvp in _counters.OrderBy(x => x.Key))
                {
                    result.Add(kvp.Value.Clone());
                }

                return result;
            }
        }

        public void IncrementCounter(string name, long n, bool forwardDisabled = false)
        {
            if (_forwardCountersToCollection != null && !forwardDisabled)
                _forwardCountersToCollection.IncrementCounter(name, n);

            lock (_counters)
            {
                if (!_counters.TryGetValue(name, out var value))
                {
                    value = new StatCounter()
                    {
                        Name = name,
                        Code = GetCounterCode(name),
                        IsDebug = false,
                    };

                    _counters[name] = value;
                }

                value.Value += n;
                value.IsDebug = false;
            }
        }

        public void IncrementDebugCounter(string name, long n, bool forwardDisabled = false)
        {
            if (_forwardCountersToCollection != null && !forwardDisabled)
                _forwardCountersToCollection.IncrementDebugCounter(name, n);

            lock (_counters)
            {
                if (!_counters.TryGetValue(name, out var value))
                {
                    value = new StatCounter()
                    {
                        Name = name,
                        Code = GetCounterCode(name),
                        IsDebug = true,
                    };

                    _counters[name] = value;
                }

                value.Value += n;
                value.IsDebug = true;
            }
        }

        private static string GetCounterCode(string name)
        {
            return Math.Abs(name.GetHashCode(StringComparison.InvariantCultureIgnoreCase)).ToString("D", CultureInfo.InvariantCulture);
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