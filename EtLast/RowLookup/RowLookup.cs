namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class RowLookup : ICountableLookup
    {
        /// <summary>
        /// Default false.
        /// </summary>
        public bool Invariant { get; }

        public int Count { get; private set; }
        public IEnumerable<string> Keys => _dictionary.Keys;

        private readonly Dictionary<string, object> _dictionary;

        public RowLookup(bool invariant = false)
        {
            Invariant = invariant;
            _dictionary = invariant
                ? new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase)
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
                var result = list.Where(filter).ToList();
                return result.Count > 0
                    ? result
                    : null;
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

            return (entry is List<IReadOnlySlimRow> list)
                ? list[0]
                : entry as IReadOnlySlimRow;
        }

        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}