namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;
    using System.Text;
    using FizzCode.EtLast;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class AssertExtensions
    {
        private static readonly ColumnBasedRowEqualityComparer RowComparer = new ColumnBasedRowEqualityComparer();

        public static void RowsAreEqual(this Assert assert, IRow expected, IRow actual)
        {
            if (assert == null)
                throw new System.ArgumentNullException(nameof(assert));

            if (!RowComparer.Equals(expected, actual))
            {
                var comparisonString = RowComparerHelper.CompareMessage(expected, actual);

                throw new AssertFailedException($"Assert.That.Equals failed.\r\n\r\nExpected | Actual:\r\n{comparisonString}");
            }
        }

        public static void RowsAreEqual(this Assert assert, List<IRow> expecteds, List<IRow> actuals)
        {
            if (assert == null)
                throw new System.ArgumentNullException(nameof(assert));

            if (expecteds.Count != actuals.Count)
                throw new AssertFailedException("Assert.That.Equals failed.\r\n\r\nDifferent amount of rows.");

            expecteds = RowHelper.OrderRows(expecteds);
            actuals = RowHelper.OrderRows(actuals);

            var equals = true;
            var i = 0;
            var comparisonResult = new StringBuilder();

            foreach (var expected in expecteds)
            {
                var actual = actuals[i++];

                if (!RowComparer.Equals(expected, actual))
                {
                    equals = false;
                    comparisonResult.AppendLine(RowComparerHelper.CompareMessage(expected, actual));
                }
            }

            if (!equals)
                throw new AssertFailedException($"Assert.That.Equals failed.\r\n\r\nDiffering rows, Expected | Actual:\r\n{comparisonResult}");
        }
    }
}
