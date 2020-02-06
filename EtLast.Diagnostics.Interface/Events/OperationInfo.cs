namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Globalization;
    using System.Text.Json.Serialization;

    public class OperationInfo
    {
        [JsonPropertyName("t")]
        public string Type { get; set; }

        [JsonPropertyName("nu")]
        public int Number { get; set; }

        [JsonPropertyName("na")]
        public string InstanceName { get; set; }

        public string ToDisplayValue()
        {
            return Number.ToString("D2", CultureInfo.InvariantCulture) + "." + (InstanceName ?? Type);
        }
    }
}