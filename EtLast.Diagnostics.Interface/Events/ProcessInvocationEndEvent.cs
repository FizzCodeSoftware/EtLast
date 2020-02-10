namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class ProcessInvocationEndEvent : AbstractEvent
    {
        [JsonPropertyName("uid")]
        public int InvocationUID { get; set; }

        [JsonPropertyName("t")]
        public long ElapsedMilliseconds { get; set; }
    }
}