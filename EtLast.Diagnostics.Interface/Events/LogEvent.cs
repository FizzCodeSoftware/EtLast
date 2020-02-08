namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;
    using FizzCode.EtLast;

    public class LogEvent : AbstractEvent
    {
        [JsonPropertyName("p")]
        public int? ProcessInvocationUID { get; set; }

        [JsonPropertyName("t")]
        public string Text { get; set; }

        [JsonPropertyName("s")]
        public LogSeverity Severity { get; set; }

        [JsonPropertyName("a")]
        public NamedArgument[] Arguments { get; set; }

        [JsonPropertyName("fo")]
        public bool ForOps { get; set; }
    }
}