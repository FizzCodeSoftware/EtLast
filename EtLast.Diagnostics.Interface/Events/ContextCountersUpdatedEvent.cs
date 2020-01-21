namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;

    public class ContextCountersUpdatedEvent : AbstractEvent
    {
        public List<Counter> Counters { get; set; }
    }

    public class Counter
    {
        public string Name { get; set; }
        public string Code { get; set; }
        public long Value { get; set; }
        public StatCounterValueType ValueType { get; set; }

        public string ValueToString => ValueType switch
        {
            StatCounterValueType.Numeric => Argument.LongToString(Value),
            StatCounterValueType.TimeSpan => Argument.TimeSpanToString(TimeSpan.FromMilliseconds(Value)),
            _ => throw new NotSupportedException(nameof(ValueType) + "." + ValueType.ToString()),
        };
    }
}