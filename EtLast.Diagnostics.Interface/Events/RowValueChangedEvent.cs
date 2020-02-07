﻿namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class RowValueChangedEvent : AbstractEvent
    {
        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("p")]
        public int? ProcessUid { get; set; }

        [JsonPropertyName("c")]
        public string Column { get; set; }

        [JsonPropertyName("v")]
        public Argument CurrentValue { get; set; }

        [JsonPropertyName("o")]
        public int? OperationUid { get; set; }
    }
}