namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation;

[TestClass]
public class ContinuousAggregationMutatorTests
{
    private static ISequenceBuilder GetBuilder(IEtlContext context, IContinuousAggregationOperation op, ITypeConverter converter)
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
            .AggregateContinuously(new ContinuousAggregationMutator(context)
            {
                KeyGenerator = row => row.GenerateKey("name"),
                FixColumns = new()
                {
                    ["name"] = null
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
    public void NoKeyDecimalAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .ConvertValue(new InPlaceConvertMutator(context)
            {
                Columns = new[] { "age", "height" },
                TypeConverter = new DecimalConverter(),
            })
            .AggregateContinuously(new ContinuousAggregationMutator(context)
            {
                Operation = new ContinuousGroupByOperation()
                    .AddDecimalMin("height", "min-height")
                    .AddDecimalMax("height", "max-height"),
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["min-height"] = 140m, ["max-height"] = 190m } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDecimalAverage("height"), new DecimalConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDecimalMax("age"), new DecimalConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDecimalMin("age"), new DecimalConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDecimalSum("age"), new DecimalConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDoubleAverage("height"), new DoubleConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleAverageIgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDoubleAverageIgnoreNull("age"), new DoubleConverter());
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["age"] = 14d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["age"] = 8d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["age"] = 27d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["age"] = 39d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["age"] = -3d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake" } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDoubleMax("age"), new DoubleConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDoubleMin("age"), new DoubleConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddDoubleSum("age"), new DoubleConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntAverage("height"), null);
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntMax("age"), null);
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntMin("age"), null);
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntSum("age"), null);
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void LongAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddLongAverage("height"), new LongConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void LongMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddLongMax("age"), new LongConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void LongMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddLongMin("age"), new LongConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void LongSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddLongSum("age"), new LongConverter());
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
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void TypeConversionError()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddLongSum("age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.InvocationContext.Exceptions.Count);
        Assert.IsTrue(result.Process.InvocationContext.Exceptions[0] is ContinuousAggregationException);
    }

    [TestMethod]
    public void IntCount()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntCount("count"), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["count"] = 2 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["count"] = 1 } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntCountWhenNotNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntCountWhenNotNull("count", "age"), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["count"] = 2 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake" } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntNumberOfDistinctKeys_IgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntNumberOfDistinctKeys("count", row => row.GenerateKey("eyeColor")), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake" } });
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }

    [TestMethod]
    public void IntNumberOfDistinctKeys_CountNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(context, new ContinuousGroupByOperation().AddIntNumberOfDistinctKeys("count", row => row.GenerateKey("eyeColor") ?? "\0NULL\0"), null);
        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "A", ["count"] = 2 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "B", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "C", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "D", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "E", ["count"] = 1 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["name"] = "fake", ["count"] = 1 } });
        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }
}
