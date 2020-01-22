namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    public class RowCreatedEvent : AbstractEvent
    {
        [JsonPropertyName("p")]
        public int ProcessUid { get; set; }

        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("v")]
        public List<NamedArgument> Values { get; set; }
    }
}