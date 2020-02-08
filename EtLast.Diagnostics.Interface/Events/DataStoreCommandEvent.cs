namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class DataStoreCommandEvent : AbstractEvent
    {
        [JsonPropertyName("p")]
        public int ProcessInvocationUID { get; set; }

        [JsonPropertyName("c")]
        public string Command { get; set; }

        [JsonPropertyName("l")]
        public string Location { get; set; }

        [JsonPropertyName("a")]
        public NamedArgument[] Arguments { get; set; }
    }
}