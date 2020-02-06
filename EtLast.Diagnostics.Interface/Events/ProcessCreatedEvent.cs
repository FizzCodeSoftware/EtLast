namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class ProcessCreatedEvent : AbstractEvent
    {
        [JsonPropertyName("u")]
        public int Uid { get; set; }

        [JsonPropertyName("ty")]
        public string Type { get; set; }

        [JsonPropertyName("n")]
        public string Name { get; set; }

        [JsonPropertyName("to")]
        public string Topic { get; set; }
    }
}