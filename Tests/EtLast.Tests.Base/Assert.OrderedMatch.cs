namespace FizzCode.EtLast.Tests;

public static class OrderedMatchHelper
{
    public static void ExactMatch(this Assert assert, List<ISlimRow> rows, List<CaseInsensitiveStringKeyDictionary<object>> referenceRows)
    {
        if (assert is null)
            throw new ArgumentNullException(nameof(assert));

        Assert.AreEqual(referenceRows.Count, rows.Count, "AssertValuesAreEqual failed, number of expected rows are not equal to actual number of rows.");
        for (var i = 0; i < referenceRows.Count; i++)
        {
            var referenceRow = referenceRows[i];
            var row = rows[i];

            foreach (var kvp in referenceRow)
            {
                var expectedValue = kvp.Value;
                Assert.AreNotEqual(null, expectedValue, "expectedValue is null in row " + i);
                var value = row[kvp.Key];
                AssertValuesAreEqual(expectedValue, value, kvp.Key, i);
            }

            foreach (var kvp in row.Values)
            {
                var expectedValue = kvp.Value;
                var value = referenceRow[kvp.Key];
                AssertValuesAreEqual(expectedValue, value, kvp.Key, i);
            }
        }
    }

    private static void AssertValuesAreEqual(object expected, object actual, string key, int row)
    {
        var areEqual = DefaultValueComparer.ValuesAreEqual(actual, expected);
        Assert.IsTrue(areEqual, "AssertValuesAreEqual failed. Expected:<" + expected + ">.Actual:<" + actual + ">, Key: " + key + ", in row " + row + ".");
    }
}