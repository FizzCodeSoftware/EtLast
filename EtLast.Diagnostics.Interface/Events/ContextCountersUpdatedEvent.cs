namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;

    public class ContextCountersUpdatedEvent : AbstractEvent
    {
        public Counter[] Counters { get; set; }
    }

    public class Counter
    {
        public string Name { get; set; }
        public long Value { get; set; }
        public StatCounterValueType ValueType { get; set; }

        public string ValueToString => ValueType switch
        {
            StatCounterValueType.Numeric => FormattingHelpers.LongToString(Value),
            StatCounterValueType.TimeSpan => FormattingHelpers.TimeSpanToString(TimeSpan.FromMilliseconds(Value)),
            _ => throw new NotSupportedException(nameof(ValueType) + "." + ValueType.ToString()),
        };
    }
}