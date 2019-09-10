namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class AdditionalData
    {
        public Dictionary<string, object> Dictionary { get; set; }

        public T GetData<T>(string key)
        {
            Dictionary.TryGetValue(key, out var value);
            return (T)value;
        }
    }
}