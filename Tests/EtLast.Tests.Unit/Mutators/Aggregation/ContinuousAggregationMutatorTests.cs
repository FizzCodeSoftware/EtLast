namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ContinuousAggregationMutatorTests
    {
        private static ProcessBuilder GetBuilder(ITopic topic, IContinuousAggregationOperation op, ITypeConverter converter)
        {
            return new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    converter == null ? null : new InPlaceConvertMutator(topic, null)
                    {
                        Columns = new[] { "age", "height" },
                        TypeConverter = converter,
                    },
                    new ContinuousAggregationMutator(topic, null)
                    {
                        KeyGenerator = row => row.GenerateKey("name"),
                        FixColumns = ColumnCopyConfiguration.StraightCopy("name"),
                        Operation = op,
                    },
                },
            };
        }

        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<ContinuousAggregationMutator>();
        }

        [TestMethod]
        public void DecimalAverage()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDecimalAverage("height"), new DecimalConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DecimalMax()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDecimalMax("age"), new DecimalConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DecimalMin()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDecimalMin("age"), new DecimalConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DecimalSum()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDecimalSum("age"), new DecimalConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DoubleAverage()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDoubleAverage("height"), new DoubleConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DoubleMax()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDoubleMax("age"), new DoubleConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DoubleMin()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDoubleMin("age"), new DoubleConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void DoubleSum()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddDoubleSum("age"), new DoubleConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IntAverage()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddIntAverage("height"), null);
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IntMax()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddIntMax("age"), null);
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IntMin()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddIntMin("age"), null);
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IntSum()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddIntSum("age"), null);
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void LongAverage()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddLongAverage("height"), new LongConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void LongMax()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddLongMax("age"), new LongConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void LongMin()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddLongMin("age"), new LongConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void LongSum()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddLongSum("age"), new LongConverter());
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
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void TypeConversionError()
        {
            var topic = TestExecuter.GetTopic();
            var builder = GetBuilder(topic, new ContinuousGroupByOperation().AddLongSum("age"), null);
            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ContinuousAggregationException);
        }
    }
}