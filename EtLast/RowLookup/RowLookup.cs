namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public sealed class RowLookup : ICountableLookup
    {
        /// <summary>
        /// Default false.
        /// </summary>
        public bool IgnoreCase { get; }

        public int Count { get; private set; }
        public IEnumerable<string> Keys => _dictionary.Keys;

        private readonly Dictionary<string, object> _dictionary;

        public RowLookup(bool ignoreCase = false)
        {
            IgnoreCase = ignoreCase;
            _dictionary = ignoreCase
                ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object>();
        }

        public void AddRow(string key, IReadOnlySlimRow row)
        {
            if (string.IsNullOrEmpty(key))
                return;

            Count++;
            if (_dictionary.TryGetValue(key, out var entry))
            {
                if (entry is List<IReadOnlySlimRow> list)
                {
                    list.Add(row);
                }
                else
                {
                    _dictionary[key] = new List<IReadOnlySlimRow>()
                    {
                        entry as IReadOnlySlimRow,
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

            return (entry is List<IReadOnlySlimRow> list)
                ? list.Count
                : 1;
        }

        public List<IReadOnlySlimRow> GetManyByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            return (entry is List<IReadOnlySlimRow> list)
                ? list
                : new List<IReadOnlySlimRow>() { entry as IReadOnlySlimRow };
        }

        public List<IReadOnlySlimRow> GetManyByKey(string key, Func<IReadOnlySlimRow, bool> filter)
        {
            if (filter == null)
                return GetManyByKey(key);

            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            if (entry is List<IReadOnlySlimRow> list)
            {
                List<IReadOnlySlimRow> result = null;
                foreach (var x in list)
                {
                    if (filter(x))
                    {
                        (result ??= new List<IReadOnlySlimRow>()).Add(x);
                    }
                }

                return result;
            }

            var row = entry as IReadOnlySlimRow;
            return filter(row)
                ? new List<IReadOnlySlimRow> { row }
                : null;
        }

        public IReadOnlySlimRow GetSingleRowByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            if (entry is List<IReadOnlySlimRow> list)
                return list[0];

            return entry as IReadOnlySlimRow;
        }

        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}