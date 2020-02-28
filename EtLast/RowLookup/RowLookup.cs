namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class RowLookup : ICountableLookup
    {
        public int Count { get; private set; }
        private readonly Dictionary<string, object> _dictionary = new Dictionary<string, object>();

        public void AddRow(string key, IRow row)
        {
            Count++;
            if (_dictionary.TryGetValue(key, out var entry))
            {
                if (entry is List<IRow> list)
                {
                    list.Add(row);
                }
                else
                {
                    _dictionary[key] = new List<IRow>()
                    {
                        entry as IRow,
                        row,
                    };
                }
            }
            else
            {
                _dictionary[key] = row;
            }
        }

        public int GetRowCountByKey(string key)
        {
            if (key == null)
                return 0;

            if (!_dictionary.TryGetValue(key, out var entry))
                return 0;

            return (entry is List<IRow> list)
                ? list.Count
                : 1;
        }

        public List<IRow> GetManyByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            return (entry is List<IRow> list)
                ? list
                : new List<IRow>() { entry as IRow };
        }

        public List<IRow> GetManyByKey(string key, Func<IRow, bool> filter)
        {
            if (filter == null)
                return GetManyByKey(key);

            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            if (entry is List<IRow> list)
            {
                var result = list.Where(filter).ToList();
                return result.Count > 0
                    ? result
                    : null;
            }

            var row = entry as IRow;
            return filter(row)
                ? new List<IRow> { row }
                : null;
        }

        public IRow GetSingleRowByKey(string key)
        {
            if (key == null)
                return null;

            if (!_dictionary.TryGetValue(key, out var entry))
                return null;

            return (entry is List<IRow> list)
                ? list[0]
                : entry as IRow;
        }

        public void Clear()
        {
            _dictionary.Clear();
        }
    }
}