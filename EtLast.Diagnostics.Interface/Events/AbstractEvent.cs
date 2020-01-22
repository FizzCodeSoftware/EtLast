namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public abstract class AbstractEvent
    {
        [JsonPropertyName("ts")]
        public long Timestamp { get; set; }
    }
}