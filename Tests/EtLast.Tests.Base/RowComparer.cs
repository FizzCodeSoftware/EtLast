namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using FizzCode.EtLast;

    public class RowComparer
    {
        public enum RowComparerMode
        {
            Default,
            Test
        }

        private readonly RowComparerMode _rowComparerMode;

        public RowComparer() : this(RowComparerMode.Default)
        {
        }

        public RowComparer(RowComparerMode rowComparerMode)
        {
            _rowComparerMode = rowComparerMode;
        }

        public bool Equals(IRow row1, IRow row2)
        {
            if (row1 == row2)
                return true;

            if ((row1 == null && row2 != null) || (row1 != null && row2 == null))
                return false;

            foreach (var kvp in row1.Values)
            {
                if (!Equals(kvp, row2))
                    return false;
            }

            return true;
        }

        public bool Equals(KeyValuePair<string, object> kvp, IRow row2)
        {
#pragma warning disable RCS1104 // Simplify conditional expression.
            return kvp.Value == null && row2[kvp.Key] == null
                ? true
                : kvp.Value == null && row2[kvp.Key] != null
                    ? false
                    : kvp.Value.Equals(row2[kvp.Key]) || (_rowComparerMode == RowComparerMode.Default ? false : AreEqualEtlRowErrors(kvp, row2));
#pragma warning restore RCS1104 // Simplify conditional expression.
        }

        private static bool AreEqualEtlRowErrors(KeyValuePair<string, object> kvp, IRow row2)
        {
            return kvp.Value is EtlRowErrorTest etlRowErrorTest
                && row2[kvp.Key] is EtlRowError etlRowError
                && etlRowErrorTest.OriginalValue.Equals(etlRowError.OriginalValue);
        }
    }
}