namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class AdditionalData
    {
        private readonly Dictionary<string, object> _data = new Dictionary<string, object>();

        public void SetData<T>(string key, T data)
        {
            _data[key] = data;
        }

        public T GetData<T>(string key, T defaultValue)
        {
            _data.TryGetValue(key, out var value);
            if (value != null && value is T)
            {
                return (T)value;
            }

            return defaultValue;
        }
    }
}