namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class CountableOnlyLookup : ICountableLookup
    {
        public int Count { get; private set; }
        private readonly Dictionary<string, int> _dictionary = new Dictionary<string, int>();

        public void AddRow(string key, IRow row)
        {
            Count++;
            _dictionary.TryGetValue(key, out var count);
            _dictionary[key] = count + 1;
        }

        public int GetRowCountByKey(string key)
        {
            if (key == null)
                return 0;

            _dictionary.TryGetValue(key, out var count);
            return count;
        }

        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}