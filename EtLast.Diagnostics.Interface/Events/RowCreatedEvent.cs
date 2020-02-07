namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class RowCreatedEvent : AbstractEvent
    {
        [JsonPropertyName("p")]
        public int ProcessUid { get; set; }

        [JsonPropertyName("o")]
        public int? OperationUid { get; set; }

        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("v")]
        public NamedArgument[] Values { get; set; }
    }
}