namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Text;

    public class RowKeyBuilder
    {
        private readonly StringBuilder _keyBuilder = new StringBuilder();

        public string GetKey(string[] keyColumns, IReadOnlySlimRow row)
        {
            if (keyColumns.Length == 1)
            {
                var value = row[keyColumns[0]];

                return value != null
                    ? DefaultValueFormatter.Format(value)
                    : "\0";
            }

            for (var i = 0; i < keyColumns.Length; i++)
            {
                var value = row[keyColumns[i]];

                if (value != null)
                    _keyBuilder.Append(DefaultValueFormatter.Format(value));

                _keyBuilder.Append('\0');
            }

            var key = _keyBuilder.ToString();
            _keyBuilder.Clear();
            return key;
        }

        public string GetKey(List<ColumnCopyConfiguration> keyColumns, IReadOnlySlimRow row)
        {
            if (keyColumns.Count == 1)
            {
                var value = row[keyColumns[0].FromColumn];

                return value != null
                    ? DefaultValueFormatter.Format(value)
                    : "\0";
            }

            for (var i = 0; i < keyColumns.Count; i++)
            {
                var value = row[keyColumns[i].FromColumn];

                if (value != null)
                    _keyBuilder.Append(DefaultValueFormatter.Format(value));

                _keyBuilder.Append('\0');
            }

            var key = _keyBuilder.ToString();
            _keyBuilder.Clear();
            return key;
        }
    }
}