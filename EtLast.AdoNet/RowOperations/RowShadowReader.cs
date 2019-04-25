namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;

    internal class RowShadowReader : DbDataReader
    {
        public object[,] Rows { get; }
        public int RowCount { get; set; }

        private readonly string[] _columns;
        private readonly Dictionary<string, int> _columnIndexes;

        private DataTable _schemaTable;
        private int _currentIndex;
        private bool _active = true;

        public RowShadowReader(int batchSize, string[] columns, Dictionary<string, int> columnIndexes)
        {
            _columns = columns;
            _columnIndexes = columnIndexes;

            Rows = new object[batchSize, columns.Length];
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
            for (int i = 0; i < _columns.Length; i++)
            {
                rowData[0] = i;
                rowData[1] = _columns[i];
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
                if (_currentIndex < RowCount) return true;
                else _active = false;
            }

            _currentIndex = -1;
            return false;
        }

        public override int RecordsAffected => 0;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing) Shutdown();
        }

        private void Shutdown()
        {
            _active = false;
            Reset();
        }

        public override int FieldCount => _columns.Length;

        public override bool IsClosed => !_active;

        public override bool GetBoolean(int i)
        {
            return (bool)this[i];
        }

        public override byte GetByte(int i)
        {
            return (byte)this[i];
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            var value = (byte[])this[i];
            var remaining = value.Length - (int)fieldOffset;
            if (remaining <= 0) return 0;

            var count = Math.Min(length, remaining);
            Buffer.BlockCopy(value, (int)fieldOffset, buffer, bufferoffset, count);
            return count;
        }

        public override char GetChar(int i)
        {
            return (char)this[i];
        }

        public override long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            var value = (string)this[i];
            var remaining = value.Length - (int)fieldOffset;
            if (remaining <= 0) return 0;

            var count = Math.Min(length, remaining);
            value.CopyTo((int)fieldOffset, buffer, bufferOffset, count);
            return count;
        }

        protected override DbDataReader GetDbDataReader(int i)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int i)
        {
            return typeof(object).Name;
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime)this[i];
        }

        public override decimal GetDecimal(int i)
        {
            return (decimal)this[i];
        }

        public override double GetDouble(int i)
        {
            return (double)this[i];
        }

        public override Type GetFieldType(int i)
        {
            return typeof(object);
        }

        public override float GetFloat(int i)
        {
            return (float)this[i];
        }

        public override Guid GetGuid(int i)
        {
            return (Guid)this[i];
        }

        public override short GetInt16(int i)
        {
            return (short)this[i];
        }

        public override int GetInt32(int i)
        {
            return (int)this[i];
        }

        public override long GetInt64(int i)
        {
            return (long)this[i];
        }

        public override string GetName(int i)
        {
            return _columns[i];
        }

        public override int GetOrdinal(string name)
        {
            return _columnIndexes[name];
        }

        public override string GetString(int i)
        {
            return (string)this[i];
        }

        public override object GetValue(int i)
        {
            return this[i];
        }

        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        public override int GetValues(object[] values)
        {
            var rows = Rows; // cache on stack

            var count = Math.Min(values.Length, _columns.Length);
            for (int i = 0; i < count; i++) values[i] = rows[_currentIndex, i] ?? DBNull.Value;

            return count;
        }

        public override bool IsDBNull(int i)
        {
            return this[i] is DBNull;
        }

        public override object this[string name]
        {
            get
            {
                return Rows[_currentIndex, _columnIndexes[name]] ?? DBNull.Value;
            }

        }

        public override object this[int i]
        {
            get
            {
                return Rows[_currentIndex, i] ?? DBNull.Value;
            }
        }
    }
}