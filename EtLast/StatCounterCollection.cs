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

        public void IncrementCounter(string baseName, long n, bool forwardDisabled = false)
        {
            Increment(baseName, n, forwardDisabled, false, StatCounterValueType.Numeric);
        }

        public void IncrementDebugCounter(string baseName, long n, bool forwardDisabled = false)
        {
            Increment(baseName, n, forwardDisabled, true, StatCounterValueType.Numeric);
        }

        public void IncrementTimeSpan(string baseName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, false, StatCounterValueType.TimeSpan);
        }

        public void IncrementDebugTimeSpan(string baseName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, true, StatCounterValueType.TimeSpan);
        }

        internal void Increment(string name, long n, bool forwardDisabled, bool isDebug, StatCounterValueType counterType)
        {
            if (n == 0)
                return;

            if (_forwardCountersToCollection != null && !forwardDisabled)
                _forwardCountersToCollection.Increment(name, n, false, isDebug, counterType);

            lock (_counters)
            {
                if (!_counters.TryGetValue(name, out var counter))
                {
                    counter = new StatCounter()
                    {
                        Name = name,
                        Code = GetCounterCode(name),
                        IsDebug = isDebug,
                    };

                    _counters[name] = counter;
                }

                counter.ValueType = counterType;
                counter.Value += n;
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