namespace FizzCode.EtLast.Tests;

public static class OrderedMatchHelper
{
    public static void ExactMatch(this Assert assert, List<ISlimRow> rows, List<CaseInsensitiveStringKeyDictionary<object>> referenceRows)
    {
        if (assert is null)
            throw new ArgumentNullException(nameof(assert));

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
                var value = referenceRow[kvp.Key];
                Assert.IsTrue(DefaultValueComparer.ValuesAreEqual(value, expectedValue));
            }
        }
    }
}
