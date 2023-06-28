namespace FizzCode.EtLast.Tests.Unit.Rows;

[TestClass]
public class RowTests
{
    [TestMethod]
    public void ToDebugStringStartsWithUid()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "x",
        };

        var row = context.CreateRow(null, initialValues);
        var result = row.ToDebugString();

        Assert.IsTrue(result.StartsWith("uid", StringComparison.InvariantCultureIgnoreCase));
    }

    [TestMethod]
    public void KeyCaseIgnored()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["date"] = new DateTime(2020, 02, 20, 12, 12, 0, 666),
        };

        var row = context.CreateRow(null, initialValues);
        var result = row["DATE"];
        Assert.AreEqual(new DateTime(2020, 02, 20, 12, 12, 0, 666), result);
    }

    [TestMethod]
    public void SingleNullColumnResultsNullKey()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>();

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("name");
        Assert.IsNull(result);
    }

    [TestMethod]
    public void MultipleNullColumnsResultsNonNullKey()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>();

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("id", "name");
        Assert.IsNotNull(result);
    }

    [TestMethod]
    public void DateTimeKeyIsInvariantWithMilliseconds()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["date"] = new DateTime(2020, 02, 20, 12, 12, 0, 666),
        };

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("date");
        Assert.AreEqual("2020.02.20 12:12:00.6660000", result);
    }

    [TestMethod]
    public void DateTimeOffsetKeyIsInvariantWithMilliseconds()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["dto"] = new DateTimeOffset(2020, 02, 20, 12, 12, 0, 666, new TimeSpan(2, 0, 0)),
        };

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("dto");
        Assert.AreEqual("2020.02.20 12:12:00.6660000 +02:00", result);
    }

    [TestMethod]
    public void TimeSpanKeyIsInvariantWithDaysAndMilliseconds()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["time"] = new TimeSpan(1, 1, 0),
        };

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("time");
        Assert.AreEqual("0:01:01:00.0000000", result);
    }

    [TestMethod]
    public void IntKeyIsInvariant()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 1234567,
            ["date"] = new DateTime(2020, 02, 20, 12, 12, 0, 666),
        };

        var row = context.CreateRow(null, initialValues);
        var result = row.GenerateKey("id", "date");
        Assert.IsTrue(result.Contains("1234567", StringComparison.Ordinal));
        Assert.IsTrue(result.Contains("2020.02.20 12:12:00.6660000", StringComparison.Ordinal));
    }

    [TestMethod]
    public void HasErrorFalse()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
        };

        var row = context.CreateRow(null, initialValues);
        Assert.IsFalse(row.HasError());
    }

    [TestMethod]
    public void HasErrorTrue()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
            ["err"] = new EtlRowError(9),
        };

        var row = context.CreateRow(null, initialValues);
        Assert.IsTrue(row.HasError());
    }

    [TestMethod]
    public void NullValuesAreNotStored1()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
            ["age"] = null,
        };

        var row = context.CreateRow(null, initialValues);
        Assert.AreEqual(2, row.ColumnCount);
        Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

        row["age"] = 7;
        Assert.AreEqual(3, row.ColumnCount);
        Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

        row["name"] = null;
        Assert.AreEqual(2, row.ColumnCount);
        Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));
    }

    [TestMethod]
    public void NullValuesAreNotStored2()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var initialValues = new Dictionary<string, object>()
        {
            ["id"] = 12,
            ["name"] = "A",
        };

        var row = context.CreateRow(null, initialValues);
        Assert.AreEqual(2, row.ColumnCount);

        row["id"] = null;
        Assert.AreEqual(1, row.ColumnCount);

        row["trash"] = null;
        Assert.AreEqual(1, row.ColumnCount);
    }

    [TestMethod]
    public void IsNullOrEmptyTrue1()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        Assert.AreEqual(true, row.IsNullOrEmpty());
    }

    [TestMethod]
    public void IsNullOrEmptyTrue2()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        row["a"] = "";
        Assert.AreEqual(true, row.IsNullOrEmpty());
    }

    [TestMethod]
    public void IsNullOrEmptyTrue3()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        row["a"] = "";
        row["a"] = "x";
        row["a"] = null;
        Assert.AreEqual(true, row.IsNullOrEmpty());
    }

    [TestMethod]
    public void IsNullOrEmptyFalse()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        row["a"] = 5;
        Assert.AreEqual(false, row.IsNullOrEmpty());
    }

    [TestMethod]
    public void MergeTestWithRow()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        row["a"] = 1;
        row["b"] = "dog";
        row["c"] = 7.1d;

        var newRow = new SlimRow()
        {
            ["b"] = "cat",
            ["c"] = 7.1d,
            ["d"] = 8m,
        };

        row.MergeWith(newRow.Values);

        Assert.AreEqual(4, row.ColumnCount);
        Assert.AreEqual(1, row["a"]);
        Assert.AreEqual("cat", row["b"]);
        Assert.AreEqual(7.1d, row["c"]);
        Assert.AreEqual(8m, row["d"]);
    }

    [TestMethod]
    public void MergeTestWithValues()
    {
        var context = TestExecuter.GetContext();
        context.SetRowType<Row>();

        var row = context.CreateRow(null);
        row["a"] = 1;
        row["b"] = "dog";
        row["c"] = 7.1d;

        var newValues = new Dictionary<string, object>()
        {
            ["b"] = "cat",
            ["c"] = null,
            ["d"] = 8m,
        };

        row.MergeWith(newValues);

        Assert.AreEqual(3, row.ColumnCount);
        Assert.AreEqual(1, row["a"]);
        Assert.AreEqual("cat", row["b"]);
        Assert.IsFalse(row.HasValue("c"));
        Assert.AreEqual(8m, row["d"]);
    }
}
