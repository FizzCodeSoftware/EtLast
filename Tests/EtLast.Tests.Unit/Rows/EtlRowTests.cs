namespace FizzCode.EtLast.Tests.Unit.Rows
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EtlRowTests
    {
        [TestMethod]
        public void ToDebugStringStartsWithUid()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

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
        public void SingleNullColumnResultsNullKey()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>();

            var row = context.CreateRow(null, initialValues);
            var result = row.GenerateKey("name");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void MultipleNullColumnsResultsNonNullKey()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>();

            var row = context.CreateRow(null, initialValues);
            var result = row.GenerateKey("id", "name");
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void DateTimeKeyIsInvariantWithMilliseconds()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

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
            context.SetRowType<DictionaryRow>();

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
            context.SetRowType<DictionaryRow>();

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
            context.SetRowType<DictionaryRow>();

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
            context.SetRowType<DictionaryRow>();

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
            context.SetRowType<DictionaryRow>();

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
        public void NullValuesAreNotStored()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>()
            {
                ["id"] = 12,
                ["name"] = "A",
                ["age"] = null,
            };

            var row = context.CreateRow(null, initialValues);
            Assert.AreEqual(2, row.ColumnCount);
            Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

            row.SetValue("age", 7);
            Assert.AreEqual(3, row.ColumnCount);
            Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

            row.SetValue("name", null);
            Assert.AreEqual(2, row.ColumnCount);
            Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

            row.SetStagedValue("x", 7);
            row.ApplyStaging();
            Assert.AreEqual(3, row.ColumnCount);
            Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));

            row.SetStagedValue("x", null);
            row.ApplyStaging();
            Assert.AreEqual(2, row.ColumnCount);
            Assert.IsTrue(row.Values.All(kvp => kvp.Value != null));
        }

        [TestMethod]
        public void StagedValueIsNotVisibleUntilApplied()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>()
            {
                ["name"] = "A",
            };

            var row = context.CreateRow(null, initialValues);
            row.SetStagedValue("name", "B");

            Assert.AreEqual("A", row.GetAs<string>("name"));
            Assert.IsTrue(row.HasStaging);
        }

        [TestMethod]
        public void StagedValueIsAppliedProperly()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>()
            {
                ["name"] = "A",
            };

            var row = context.CreateRow(null, initialValues);
            row.SetStagedValue("name", "B");
            row.SetStagedValue("name", "C");
            row.ApplyStaging();

            Assert.AreEqual("C", row.GetAs<string>("name"));
            Assert.IsFalse(row.HasStaging);
        }

        [TestMethod]
        public void ApplyStagingIsIdempotent()
        {
            var context = TestExecuter.GetContext();
            context.SetRowType<DictionaryRow>();

            var initialValues = new Dictionary<string, object>()
            {
                ["name"] = "A",
            };

            var row = context.CreateRow(null, initialValues);
            row.SetStagedValue("name", "B");
            row.ApplyStaging();

            row.SetValue("name", "A");
            row.ApplyStaging(); // nothing should happen

            Assert.AreEqual("A", row.GetAs<string>("name"));
            Assert.IsFalse(row.HasStaging);
        }
    }
}