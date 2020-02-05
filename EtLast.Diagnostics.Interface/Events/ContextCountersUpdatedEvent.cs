namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Text.Json.Serialization;

    public class ContextCountersUpdatedEvent : AbstractEvent
    {
        public Counter[] Counters { get; set; }
    }

    public class Counter
    {
        [JsonPropertyName("n")]
        public string Name { get; set; }

        [JsonPropertyName("v")]
        public long Value { get; set; }

        [JsonPropertyName("t")]
        public StatCounterValueType ValueType { get; set; }

        public string ValueToString => ValueType switch
        {
            StatCounterValueType.Numeric => Argument.LongToString(Value),
            StatCounterValueType.TimeSpan => Argument.TimeSpanToString(TimeSpan.FromMilliseconds(Value)),
            _ => throw new NotSupportedException(nameof(ValueType) + "." + ValueType.ToString()),
        };
    }
}