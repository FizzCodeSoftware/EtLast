namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class OrderedMatchHelper
    {
#pragma warning disable RCS1175 // Unused this parameter.
        public static void ExactMatch(this Assert assert, List<ISlimRow> rows, List<Dictionary<string, object>> referenceRows)
#pragma warning restore RCS1175 // Unused this parameter.
        {
            Assert.AreEqual(referenceRows.Count, rows.Count);
            for (var i = 0; i < referenceRows.Count; i++)
            {
                var referenceRow = referenceRows[i];
                var row = rows[i];

                foreach (var kvp in referenceRow)
                {
                    var expectedValue = kvp.Value;
                    Assert.AreNotEqual(null, expectedValue, "wrong test data");
                    var value = row[kvp.Key];
                    Assert.IsTrue(DefaultValueComparer.ValuesAreEqual(value, expectedValue));
                }

                foreach (var kvp in row.Values)
                {
                    var expectedValue = kvp.Value;
                    referenceRow.TryGetValue(kvp.Key, out var value);
                    Assert.IsTrue(DefaultValueComparer.ValuesAreEqual(value, expectedValue));
                }
            }
        }
    }
}