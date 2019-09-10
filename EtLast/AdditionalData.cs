namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class AdditionalData
    {
        public Dictionary<string, object> Data { get; set; }

        public T ByKey<T>(string key)
        {
            Data.TryGetValue(key, out var value);
            return (T)value;
        }
    }
}