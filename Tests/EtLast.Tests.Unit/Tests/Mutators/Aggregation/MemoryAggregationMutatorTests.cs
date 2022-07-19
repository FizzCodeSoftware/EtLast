namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation;

[TestClass]
public class MemoryAggregationMutatorTests
{
    private static ISequenceBuilder GetBuilder(IEtlContext context, IMemoryAggregationOperation op, ITypeConverter converter)
    {
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context));

        if (converter != null)
        {
            builder = builder
                .ConvertValue(new InPlaceConvertMutator(context)
                {
                    Columns = new[] { "age", "height" },
                    TypeConverter = converter,
                });
        }

        return builder
            .Aggregate(new MemoryAggregationMutator(context)
            {
                KeyGenerator = row => row.GenerateKey("name"),
                FixColumns = new()
                {
                    ["name"] = null,
                },
                Operation = op,
            });
    }

    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<ContinuousAggregationMutator>();
    }

    [TestMethod]
    public void DecimalAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDecimalAverage("height"), new DecimalConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["height"] = 150m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["height"] = 190m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["height"] = 170m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["height"] = 160m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["height"] = 160m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["height"] = 140m } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DecimalMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDecimalMax("age"), new DecimalConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 17m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0m } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DecimalMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDecimalMin("age"), new DecimalConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 11m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0m } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DecimalSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDecimalSum("age"), new DecimalConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 28m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3m },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0m } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DoubleAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDoubleAverage("height"), new DoubleConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["height"] = 150d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["height"] = 190d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["height"] = 170d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["height"] = 140d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DoubleMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDoubleMax("age"), new DoubleConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 17d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DoubleMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDoubleMin("age"), new DoubleConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 11d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void DoubleSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddDoubleSum("age"), new DoubleConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 28d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void IntAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddIntAverage("height"), null);
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["height"] = 150d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["height"] = 190d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["height"] = 170d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["height"] = 140d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void IntMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddIntMax("age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 17 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0 } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void IntMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddIntMin("age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 11 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0 } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void IntSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddIntSum("age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 28 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0 } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void LongAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddLongAverage("height"), new LongConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["height"] = 150d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["height"] = 190d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["height"] = 170d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["height"] = 160d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["height"] = 140d } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void LongMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddLongMax("age"), new LongConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 17L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0L } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void LongMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddLongMin("age"), new LongConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 11L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0L } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void LongSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddLongSum("age"), new LongConverter());
        var result = TestExecuter.Execute(builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ColumnCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 28L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3L },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["age"] = 0L } });
        var exceptions = context.GetExceptions();
        Assert.AreEqual(0, exceptions.Count);
    }

    [TestMethod]
    public void TypeConversionError()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new MemoryGroupByOperation().AddLongSum("age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        var exceptions = context.GetExceptions();
        Assert.AreEqual(1, exceptions.Count);
        Assert.IsTrue(exceptions[0] is MemoryAggregationException);
    }
}
