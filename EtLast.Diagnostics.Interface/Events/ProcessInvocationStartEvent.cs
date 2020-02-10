namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class ProcessInvocationStartEvent : AbstractEvent
    {
        [JsonPropertyName("uid")]
        public int InvocationUID { get; set; }

        [JsonPropertyName("iuid")]
        public int InstanceUID { get; set; }

        [JsonPropertyName("c")]
        public int InvocationCounter { get; set; }

        [JsonPropertyName("ty")]
        public string Type { get; set; }

        [JsonPropertyName("k")]
        public ProcessKind Kind { get; set; }

        [JsonPropertyName("n")]
        public string Name { get; set; }

        [JsonPropertyName("to")]
        public string Topic { get; set; }

        [JsonPropertyName("cuid")]
        public int? CallerInvocationUID { get; set; }
    }
}