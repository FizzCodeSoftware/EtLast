using System;
using System.Collections.Generic;
using System.Linq;

namespace FizzCode.EtLast
{
    public enum StatCounterValueType { Numeric, TimeSpan }

    public class StatCounter
    {
        public string Name { get; internal set; }

        public string Code { get; internal set; }
        public bool IsDebug { get; internal set; }

        public StatCounterValue Value { get; } = new StatCounterValue();

        public Dictionary<string, StatCounterValue> SubValues { get; internal set; }

        public StatCounter Clone()
        {
            var counter = new StatCounter()
            {
                Name = Name,
                Code = Code,
                IsDebug = IsDebug,
                SubValues = SubValues?.ToDictionary(x => x.Key, x => new StatCounterValue()
                {
                    Value = x.Value.Value,
                    ValueType = x.Value.ValueType,
                }),
            };

            counter.Value.Value = Value.Value;
            counter.Value.ValueType = Value.ValueType;

            return counter;
        }
    }

    public class StatCounterValue
    {
        public long Value { get; internal set; }
        public StatCounterValueType ValueType { get; internal set; }

        public object TypedValue => ValueType switch
        {
            StatCounterValueType.Numeric => Value,
            StatCounterValueType.TimeSpan => TimeSpan.FromMilliseconds(Value),
            _ => throw new NotSupportedException(nameof(ValueType) + "." + ValueType.ToString()),
        };
    }
}