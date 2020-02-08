namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class RowOwnerChangedEvent : AbstractEvent
    {
        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("pp")]
        public int PreviousProcessInvocationUID { get; set; }

        [JsonPropertyName("np")]
        public int? NewProcessInvocationUID { get; set; }
    }
}