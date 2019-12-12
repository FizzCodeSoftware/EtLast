using System;

namespace FizzCode.EtLast
{
    public enum StatCounterValueType { Numeric, TimeSpan }

    public class StatCounter
    {
        public string Name { get; internal set; }

        public string Code { get; internal set; }
        public bool IsDebug { get; internal set; }
        public long Value { get; internal set; }
        public StatCounterValueType ValueType { get; internal set; }

        public object TypedValue => ValueType switch
        {
            StatCounterValueType.Numeric => Value,
            StatCounterValueType.TimeSpan => TimeSpan.FromMilliseconds(Value),
            _ => throw new NotSupportedException(nameof(ValueType) + "." + ValueType.ToString()),
        };

        public StatCounter Clone()
        {
            return new StatCounter()
            {
                Name = Name,
                Code = Code,
                IsDebug = IsDebug,
                Value = Value,
                ValueType = ValueType,
            };
        }
    }
}