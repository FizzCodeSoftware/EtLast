namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class RowValueChangedEvent : AbstractEvent
    {
        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("p")]
        public int? ProcessInvocationUID { get; set; }

        [JsonPropertyName("v")]
        public NamedArgument[] Values { get; set; }
    }
}