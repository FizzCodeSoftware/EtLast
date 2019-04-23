namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using FizzCode.EtLast;

    public static class AssertExtensions
    {
        public static void Equals(this Assert assert, IRow expected, IRow actual)
        {
            if (!RowComparer.Equals(expected, actual))
            {
                string comparisonString = RowComparerHelper.CompareMessage(expected, actual);

                throw new AssertFailedException($"Assert.That.Equals failed.\r\n\r\nExpected | Actual:\r\n{comparisonString}");
            }
        }

        public static void Equals(this Assert assert, IRow expected, object[] rowElements)
        {
            assert.Equals(expected, RowHelper.CreateRow(rowElements));
        }

        public static void Equals(this Assert assert, List<IRow> expecteds, params object[][] actualParams)
        {
            List<IRow> actuals = new List<IRow>();
            foreach (object[] rowElements in actualParams)
            {
                actuals.Add(RowHelper.CreateRow(rowElements));
            }

            assert.Equals(expecteds, actuals);
        }

        public static void Equals(this Assert assert, List<IRow> expecteds, List<IRow> actuals)
        {
            expecteds = RowHelper.OrderRows(expecteds);
            actuals = RowHelper.OrderRows(actuals);

            bool equals = true;
            int i = 0;
            StringBuilder comparisonResult = new StringBuilder();

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
