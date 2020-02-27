namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class OrderedMatchHelper
    {
#pragma warning disable RCS1175 // Unused this parameter.
        public static void OrderedMatch(this Assert assert, TestExecuterResult result, List<Dictionary<string, object>> expected)
#pragma warning restore RCS1175 // Unused this parameter.
        {
            Assert.AreEqual(expected.Count, result.MutatedRows.Count);
            for (var i = 0; i < expected.Count; i++)
            {
                var expectedValues = expected[i];
                var row = result.MutatedRows[i];
                foreach (var kvp in expectedValues)
                {
                    var expectedValue = kvp.Value;
                    var value = row[kvp.Key];
                    Assert.IsTrue(RowValueComparer.ValuesAreEqual(value, expectedValue));
                }
            }
        }
    }
}