namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class OperationInfo
    {
        [JsonPropertyName("t")]
        public string Type { get; set; }

        [JsonPropertyName("nu")]
        public int? Number { get; set; }

        [JsonPropertyName("na")]
        public string Name { get; set; }
    }
}