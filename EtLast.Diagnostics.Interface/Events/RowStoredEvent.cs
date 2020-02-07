﻿namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    public class RowStoredEvent : AbstractEvent
    {
        [JsonPropertyName("r")]
        public int RowUid { get; set; }

        [JsonPropertyName("p")]
        public int ProcessUid { get; set; }

        [JsonPropertyName("o")]
        public int? OperationUid { get; set; }

        [JsonPropertyName("l")]
        public List<KeyValuePair<string, string>> Locations { get; set; }
    }
}