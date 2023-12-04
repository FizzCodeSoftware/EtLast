namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation;

[TestClass]
public class ContinuousAggregationMutatorTests
{
    private static ISequenceBuilder GetBuilder(IContinuousAggregationOperation op, ITypeConverter converter)
    {
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person());

        if (converter != null)
        {
            builder = builder
                .ConvertInPlace("age", "height").WithTypeConverter(converter).KeepNull().KeepInvalid();
        }

        return builder
            .AggregateContinuously(new ContinuousAggregationMutator()
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
            .ReadFrom(TestData.Person())
            .ConvertInPlace("age", "height").ToDecimal().KeepNull().KeepInvalid()
            .AggregateContinuously(new ContinuousAggregationMutator()
            {
                KeyGenerator = null,
                FixColumns = null,
                Operation = new ContinuousGroupByOperation()
                    .AddDecimalMin("height", "min-height")
                    .AddDecimalMax("height", "max-height"),
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["min-height"] = 140m, ["max-height"] = 190m } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDecimalAverage("height"), new DecimalConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["height"] = 150m },
            new() { ["name"] = "B", ["height"] = 190m },
            new() { ["name"] = "C", ["height"] = 170m },
            new() { ["name"] = "D", ["height"] = 160m },
            new() { ["name"] = "E", ["height"] = 160m },
            new() { ["name"] = "fake", ["height"] = 140m } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDecimalMax("age"), new DecimalConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 17m },
            new() { ["name"] = "B", ["age"] = 8m },
            new() { ["name"] = "C", ["age"] = 27m },
            new() { ["name"] = "D", ["age"] = 39m },
            new() { ["name"] = "E", ["age"] = -3m },
            new() { ["name"] = "fake", ["age"] = 0m } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDecimalMin("age"), new DecimalConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 11m },
            new() { ["name"] = "B", ["age"] = 8m },
            new() { ["name"] = "C", ["age"] = 27m },
            new() { ["name"] = "D", ["age"] = 39m },
            new() { ["name"] = "E", ["age"] = -3m },
            new() { ["name"] = "fake", ["age"] = 0m } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DecimalSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDecimalSum("age"), new DecimalConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 28m },
            new() { ["name"] = "B", ["age"] = 8m },
            new() { ["name"] = "C", ["age"] = 27m },
            new() { ["name"] = "D", ["age"] = 39m },
            new() { ["name"] = "E", ["age"] = -3m },
            new() { ["name"] = "fake", ["age"] = 0m } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDoubleAverage("height"), new DoubleConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["height"] = 150d },
            new() { ["name"] = "B", ["height"] = 190d },
            new() { ["name"] = "C", ["height"] = 170d },
            new() { ["name"] = "D", ["height"] = 160d },
            new() { ["name"] = "E", ["height"] = 160d },
            new() { ["name"] = "fake", ["height"] = 140d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleAverageIgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDoubleAverageIgnoreNull("age"), new DoubleConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 14d },
            new() { ["name"] = "B", ["age"] = 8d },
            new() { ["name"] = "C", ["age"] = 27d },
            new() { ["name"] = "D", ["age"] = 39d },
            new() { ["name"] = "E", ["age"] = -3d },
            new() { ["name"] = "fake" } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDoubleMax("age"), new DoubleConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 17d },
            new() { ["name"] = "B", ["age"] = 8d },
            new() { ["name"] = "C", ["age"] = 27d },
            new() { ["name"] = "D", ["age"] = 39d },
            new() { ["name"] = "E", ["age"] = -3d },
            new() { ["name"] = "fake", ["age"] = 0d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDoubleMin("age"), new DoubleConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 11d },
            new() { ["name"] = "B", ["age"] = 8d },
            new() { ["name"] = "C", ["age"] = 27d },
            new() { ["name"] = "D", ["age"] = 39d },
            new() { ["name"] = "E", ["age"] = -3d },
            new() { ["name"] = "fake", ["age"] = 0d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void DoubleSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddDoubleSum("age"), new DoubleConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 28d },
            new() { ["name"] = "B", ["age"] = 8d },
            new() { ["name"] = "C", ["age"] = 27d },
            new() { ["name"] = "D", ["age"] = 39d },
            new() { ["name"] = "E", ["age"] = -3d },
            new() { ["name"] = "fake", ["age"] = 0d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntAverage("height"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["height"] = 150d },
            new() { ["name"] = "B", ["height"] = 190d },
            new() { ["name"] = "C", ["height"] = 170d },
            new() { ["name"] = "D", ["height"] = 160d },
            new() { ["name"] = "E", ["height"] = 160d },
            new() { ["name"] = "fake", ["height"] = 140d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntMax("age"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 17 },
            new() { ["name"] = "B", ["age"] = 8 },
            new() { ["name"] = "C", ["age"] = 27 },
            new() { ["name"] = "D", ["age"] = 39 },
            new() { ["name"] = "E", ["age"] = -3 },
            new() { ["name"] = "fake", ["age"] = 0 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntMin("age"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 11 },
            new() { ["name"] = "B", ["age"] = 8 },
            new() { ["name"] = "C", ["age"] = 27 },
            new() { ["name"] = "D", ["age"] = 39 },
            new() { ["name"] = "E", ["age"] = -3 },
            new() { ["name"] = "fake", ["age"] = 0 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntSum("age"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 28 },
            new() { ["name"] = "B", ["age"] = 8 },
            new() { ["name"] = "C", ["age"] = 27 },
            new() { ["name"] = "D", ["age"] = 39 },
            new() { ["name"] = "E", ["age"] = -3 },
            new() { ["name"] = "fake", ["age"] = 0 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void LongAverage()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddLongAverage("height"), new LongConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["height"] = 150d },
            new() { ["name"] = "B", ["height"] = 190d },
            new() { ["name"] = "C", ["height"] = 170d },
            new() { ["name"] = "D", ["height"] = 160d },
            new() { ["name"] = "E", ["height"] = 160d },
            new() { ["name"] = "fake", ["height"] = 140d } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void LongMax()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddLongMax("age"), new LongConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 17L },
            new() { ["name"] = "B", ["age"] = 8L },
            new() { ["name"] = "C", ["age"] = 27L },
            new() { ["name"] = "D", ["age"] = 39L },
            new() { ["name"] = "E", ["age"] = -3L },
            new() { ["name"] = "fake", ["age"] = 0L } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void LongMin()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddLongMin("age"), new LongConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 11L },
            new() { ["name"] = "B", ["age"] = 8L },
            new() { ["name"] = "C", ["age"] = 27L },
            new() { ["name"] = "D", ["age"] = 39L },
            new() { ["name"] = "E", ["age"] = -3L },
            new() { ["name"] = "fake", ["age"] = 0L } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void LongSum()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddLongSum("age"), new LongConverter());
        var result = TestExecuter.Execute(context, builder);
        Assert.IsTrue(result.MutatedRows.All(x => x.ValueCount == 2));
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["age"] = 28L },
            new() { ["name"] = "B", ["age"] = 8L },
            new() { ["name"] = "C", ["age"] = 27L },
            new() { ["name"] = "D", ["age"] = 39L },
            new() { ["name"] = "E", ["age"] = -3L },
            new() { ["name"] = "fake", ["age"] = 0L } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void TypeConversionError()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddLongSum("age"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is ContinuousAggregationException);
    }

    [TestMethod]
    public void IntCount()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntCount("count"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["count"] = 2 },
            new() { ["name"] = "B", ["count"] = 1 },
            new() { ["name"] = "C", ["count"] = 1 },
            new() { ["name"] = "D", ["count"] = 1 },
            new() { ["name"] = "E", ["count"] = 1 },
            new() { ["name"] = "fake", ["count"] = 1 } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntCountWhenNotNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntCountWhenNotNull("count", "age"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["count"] = 2 },
            new() { ["name"] = "B", ["count"] = 1 },
            new() { ["name"] = "C", ["count"] = 1 },
            new() { ["name"] = "D", ["count"] = 1 },
            new() { ["name"] = "E", ["count"] = 1 },
            new() { ["name"] = "fake" } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntNumberOfDistinctKeys_IgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntNumberOfDistinctKeys("count", row => row.GenerateKey("eyeColor")), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["count"] = 1 },
            new() { ["name"] = "B" },
            new() { ["name"] = "C", ["count"] = 1 },
            new() { ["name"] = "D", ["count"] = 1 },
            new() { ["name"] = "E" },
            new() { ["name"] = "fake" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IntNumberOfDistinctKeys_CountNull()
    {
        var context = TestExecuter.GetContext();
        var builder = GetBuilder(new ContinuousGroupByOperation().AddIntNumberOfDistinctKeys("count", row => row.GenerateKey("eyeColor") ?? "\0NULL\0"), null);
        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["name"] = "A", ["count"] = 2 },
            new() { ["name"] = "B", ["count"] = 1 },
            new() { ["name"] = "C", ["count"] = 1 },
            new() { ["name"] = "D", ["count"] = 1 },
            new() { ["name"] = "E", ["count"] = 1 },
            new() { ["name"] = "fake", ["count"] = 1 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
