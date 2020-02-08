namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class ProcessInvocationEvent : AbstractEvent
    {
        [JsonPropertyName("uid")]
        public int InvocationUID { get; set; }

        [JsonPropertyName("iuid")]
        public int InstanceUID { get; set; }

        [JsonPropertyName("c")]
        public int InvocationCounter { get; set; }

        [JsonPropertyName("ty")]
        public string Type { get; set; }

        [JsonPropertyName("n")]
        public string Name { get; set; }

        [JsonPropertyName("to")]
        public string Topic { get; set; }
    }
}