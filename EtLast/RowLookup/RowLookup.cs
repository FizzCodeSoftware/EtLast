namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class RowLookup : ICountableLookup
    {
        public int Count { get; private set; }
        public IEnumerable<string> Keys => _dictionary.Keys;

        private readonly Dictionary<string, object> _dictionary = new Dictionary<string, object>();

        public void AddRow(string key, IReadOnlyRow row)
        {
            if (string.IsNullOrEmpty(key))
                return;

            Count++;
            if (_dictionary.TryGetValue(key, out var entry))
            {
                if (entry is List<IReadOnlyRow> list)
                {
                    list.Add(row);
                }
                else
                {
                    _dictionary[key] = new List<IReadOnlyRow>()
                    {
                        entry as IReadOnlyRow,
                        row,
                    };
                }
            }
            else
            {
                _dictionary[key] = row;
            }
        }

        public int CountByKey(string key)
        {
            if (key == null)
                return 0;

            if (!_dictionary.TryGetValue(key, out var entry))
                return 0;

            return (entry is List<IReadOnlyRow> list)
                ? list.Count
                : 1;
        }

        public List<IReadOnlyRow> GetManyByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            return (entry is List<IReadOnlyRow> list)
                ? list
                : new List<IReadOnlyRow>() { entry as IReadOnlyRow };
        }

        public List<IReadOnlyRow> GetManyByKey(string key, Func<IReadOnlyRow, bool> filter)
        {
            if (filter == null)
                return GetManyByKey(key);

            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            if (entry is List<IReadOnlyRow> list)
            {
                var result = list.Where(filter).ToList();
                return result.Count > 0
                    ? result
                    : null;
            }

            var row = entry as IReadOnlyRow;
            return filter(row)
                ? new List<IReadOnlyRow> { row }
                : null;
        }

        public IReadOnlyRow GetSingleRowByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            return (entry is List<IReadOnlyRow> list)
                ? list[0]
                : entry as IReadOnlyRow;
        }

        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}