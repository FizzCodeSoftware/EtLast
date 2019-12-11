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
            Increment(baseName, null, n, forwardDisabled, false, StatCounterValueType.Numeric);
        }

        public void IncrementDebugCounter(string baseName, long n, bool forwardDisabled = false)
        {
            Increment(baseName, null, n, forwardDisabled, true, StatCounterValueType.Numeric);
        }

        public void IncrementTimeSpan(string baseName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, null, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, false, StatCounterValueType.TimeSpan);
        }

        public void IncrementDebugTimeSpan(string baseName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, null, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, true, StatCounterValueType.TimeSpan);
        }

        public void IncrementCounter(string baseName, string subName, long n, bool forwardDisabled = false)
        {
            Increment(baseName, subName, n, forwardDisabled, false, StatCounterValueType.Numeric);
        }

        public void IncrementDebugCounter(string baseName, string subName, long n, bool forwardDisabled = false)
        {
            Increment(baseName, subName, n, forwardDisabled, true, StatCounterValueType.Numeric);
        }

        public void IncrementTimeSpan(string baseName, string subName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, subName, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, false, StatCounterValueType.TimeSpan);
        }

        public void IncrementDebugTimeSpan(string baseName, string subName, TimeSpan elapsed, bool forwardDisabled = false)
        {
            Increment(baseName, subName, Convert.ToInt64(elapsed.TotalMilliseconds), forwardDisabled, true, StatCounterValueType.TimeSpan);
        }

        internal void Increment(string name, string subName, long n, bool forwardDisabled, bool isDebug, StatCounterValueType counterType)
        {
            if (n == 0)
                return;

            if (_forwardCountersToCollection != null && !forwardDisabled)
                _forwardCountersToCollection.Increment(name, subName, n, false, isDebug, counterType);

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

                if (subName == null)
                {
                    counter.Value.ValueType = counterType;
                    counter.Value.Value += n;
                }
                else
                {
                    if (counter.SubValues == null)
                        counter.SubValues = new Dictionary<string, StatCounterValue>();

                    if (!counter.SubValues.TryGetValue(subName, out var subValue))
                    {
                        subValue = new StatCounterValue();
                        counter.SubValues[subName] = subValue;
                    }

                    subValue.ValueType = counterType;
                    subValue.Value += n;
                }
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