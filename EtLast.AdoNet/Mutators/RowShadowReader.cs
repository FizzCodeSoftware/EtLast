namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    internal sealed class RowShadowReader : DbDataReader
    {
        public object[,] Rows { get; }
        public int RowCount { get; set; }
        public Dictionary<string, int> ColumnIndexes { get; }

        private readonly string[] _dbColumns;

        private DataTable _schemaTable;
        private int _currentIndex;
        private bool _active = true;

        internal RowShadowReader(int batchSize, string[] dbColumns, Dictionary<string, int> columnIndexes)
        {
            _dbColumns = dbColumns;
            ColumnIndexes = columnIndexes;

            Rows = new object[batchSize, dbColumns.Length];
            RowCount = 0;

            CreateSchemaTable();

            Reset();
        }

        private void CreateSchemaTable()
        {
            _schemaTable = new DataTable
            {
                Columns =
                {
                    {"ColumnOrdinal", typeof(int)},
                    {"ColumnName", typeof(string)},
                    {"DataType", typeof(Type)},
                    {"ColumnSize", typeof(int)},
                    {"AllowDBNull", typeof(bool)}
                }
            };

            var rowData = new object[5];
            for (var i = 0; i < _dbColumns.Length; i++)
            {
                rowData[0] = i;
                rowData[1] = _dbColumns[i];
                rowData[2] = typeof(object);
                rowData[3] = -1;
                rowData[4] = true;
                _schemaTable.Rows.Add(rowData);
            }
        }

        public void Reset()
        {
            RowCount = 0;
            _currentIndex = -1;
            _active = true;
        }

        public void ResetCurrentIndex()
        {
            _currentIndex = -1;
            _active = true;
        }

        public override int Depth => 0;

        public override DataTable GetSchemaTable()
        {
            return _schemaTable;
        }

        public override void Close()
        {
            Shutdown();
        }

        public override bool HasRows => _active;

        public override bool NextResult()
        {
            _active = false;
            return false;
        }

        public override bool Read()
        {
            if (_active)
            {
                _currentIndex++;
                if (_currentIndex < RowCount)
                    return true;

                _active = false;
            }

            _currentIndex = -1;
            return false;
        }

        public override int RecordsAffected => 0;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
                Shutdown();
        }

        private void Shutdown()
        {
            _active = false;
            _schemaTable.Dispose();
            Reset();
        }

        public override int FieldCount => _dbColumns.Length;

        public override bool IsClosed => !_active;

        public override bool GetBoolean(int ordinal)
        {
            return (bool)this[ordinal];
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)this[ordinal];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var value = (byte[])this[ordinal];
            var remaining = value.Length - (int)dataOffset;
            if (remaining <= 0)
                return 0;

            var count = Math.Min(length, remaining);
            Buffer.BlockCopy(value, (int)dataOffset, buffer, bufferOffset, count);
            return count;
        }

        public override char GetChar(int ordinal)
        {
            return (char)this[ordinal];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var value = (string)this[ordinal];
            var remaining = value.Length - (int)dataOffset;
            if (remaining <= 0)
                return 0;

            var count = Math.Min(length, remaining);
            value.CopyTo((int)dataOffset, buffer, bufferOffset, count);
            return count;
        }

        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return typeof(object).Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)this[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)this[ordinal];
        }

        public override double GetDouble(int ordinal)
        {
            return (double)this[ordinal];
        }

        public override Type GetFieldType(int ordinal)
        {
            return typeof(object);
        }

        public override float GetFloat(int ordinal)
        {
            return (float)this[ordinal];
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid)this[ordinal];
        }

        public override short GetInt16(int ordinal)
        {
            return (short)this[ordinal];
        }

        public override int GetInt32(int ordinal)
        {
            return (int)this[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (long)this[ordinal];
        }

        public override string GetName(int ordinal)
        {
            return _dbColumns[ordinal];
        }

        public override int GetOrdinal(string name)
        {
            return ColumnIndexes[name];
        }

        public override string GetString(int ordinal)
        {
            return (string)this[ordinal];
        }

        public override object GetValue(int ordinal)
        {
            return this[ordinal];
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        public override int GetValues(object[] values)
        {
            var rows = Rows; // cache on stack

            var count = Math.Min(values.Length, _dbColumns.Length);
            for (var i = 0; i < count; i++)
                values[i] = rows[_currentIndex, i] ?? DBNull.Value;

            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return this[ordinal] is DBNull;
        }

        public override object this[string name] => Rows[_currentIndex, ColumnIndexes[name]] ?? DBNull.Value;

        public override object this[int ordinal] => Rows[_currentIndex, ordinal] ?? DBNull.Value;
    }
}