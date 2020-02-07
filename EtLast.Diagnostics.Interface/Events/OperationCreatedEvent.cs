namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class OperationCreatedEvent : AbstractEvent
    {
        [JsonPropertyName("u")]
        public int Uid { get; set; }

        [JsonPropertyName("t")]
        public string Type { get; set; }

        [JsonPropertyName("n")]
        public string InstanceName { get; set; }

        [JsonPropertyName("p")]
        public int ProcessUid { get; set; }
    }
}