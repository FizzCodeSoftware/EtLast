namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast;

    public static class AssertExtensions
    {
        private static readonly RowComparer RowComparer = new RowComparer(RowComparer.RowComparerMode.Test);

        public static void RowsAreEqual(this Assert assert, IRow expected, IRow actual)
        {
            if (!RowComparer.Equals(expected, actual))
            {
                var comparisonString = RowComparerHelper.CompareMessage(expected, actual);

                throw new AssertFailedException($"Assert.That.Equals failed.\r\n\r\nExpected | Actual:\r\n{comparisonString}");
            }
        }

        public static void RowsAreEqual(this Assert assert, object[] expectedrowElements, IRow actual)
        {
            assert.RowsAreEqual(RowHelper.CreateRow(expectedrowElements), actual);
        }

        public static void RowsAreEqual1(this Assert assert, List<IRow> expecteds, params object[][] actualParams)
        {
            var actuals = new List<IRow>();
            foreach (var rowElements in actualParams)
            {
                actuals.Add(RowHelper.CreateRow(rowElements));
            }

            assert.RowsAreEqual(expecteds, actuals);
        }

        public static void RowsAreEqual(this Assert assert, List<IRow> expecteds, List<IRow> actuals)
        {
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

            if(!equals)
                throw new AssertFailedException($"Assert.That.Equals failed.\r\n\r\nDiffering rows, Expected | Actual:\r\n{comparisonResult}");
        }
    }
}
