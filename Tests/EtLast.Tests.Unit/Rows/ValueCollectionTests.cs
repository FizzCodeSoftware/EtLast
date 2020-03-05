﻿namespace FizzCode.EtLast.Tests.Unit.Rows
{
    using System;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ValueCollectionTests
    {
        [TestMethod]
        public void SingleNullColumnResultsNullKey()
        {
            var values = new ValueCollection();
            var result = values.GenerateKey("name");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void MultipleNullColumnsResultsNonNullKey()
        {
            var values = new ValueCollection();
            var result = values.GenerateKey("id", "name");
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void DateTimeKeyIsInvariantWithMilliseconds()
        {
            var values = new ValueCollection()
            {
                ["date"] = new DateTime(2020, 02, 20, 12, 12, 0, 666),
            };

            var result = values.GenerateKey("date");
            Assert.AreEqual("2020.02.20 12:12:00.6660000", result);
        }

        [TestMethod]
        public void DateTimeOffsetKeyIsInvariantWithMilliseconds()
        {
            var values = new ValueCollection()
            {
                ["dto"] = new DateTimeOffset(2020, 02, 20, 12, 12, 0, 666, new TimeSpan(2, 0, 0)),
            };

            var result = values.GenerateKey("dto");
            Assert.AreEqual("2020.02.20 12:12:00.6660000 +02:00", result);
        }

        [TestMethod]
        public void TimeSpanKeyIsInvariantWithDaysAndMilliseconds()
        {
            var values = new ValueCollection()
            {
                ["time"] = new TimeSpan(1, 1, 0),
            };

            var result = values.GenerateKey("time");
            Assert.AreEqual("0:01:01:00.0000000", result);
        }

        [TestMethod]
        public void IntKeyIsInvariant()
        {
            var values = new ValueCollection()
            {
                ["id"] = 1234567,
                ["date"] = new DateTime(2020, 02, 20, 12, 12, 0, 666),
            };

            var result = values.GenerateKey("id", "date");
            Assert.IsTrue(result.Contains("1234567", StringComparison.Ordinal));
            Assert.IsTrue(result.Contains("2020.02.20 12:12:00.6660000", StringComparison.Ordinal));
        }

        [TestMethod]
        public void HasErrorFalse()
        {
            var values = new ValueCollection()
            {
                ["id"] = 12,
                ["name"] = "A",
            };

            Assert.IsFalse(values.HasError());
        }

        [TestMethod]
        public void HasErrorTrue()
        {
            var values = new ValueCollection()
            {
                ["id"] = 12,
                ["name"] = "A",
                ["err"] = new EtlRowError(9),
            };

            Assert.IsTrue(values.HasError());
        }

        [TestMethod]
        public void NullValuesAreNotStored()
        {
            var values = new ValueCollection()
            {
                ["id"] = 12,
                ["name"] = "A",
                ["age"] = null,
            };

            Assert.AreEqual(2, values.ColumnCount);
            Assert.IsTrue(values.Values.All(kvp => kvp.Value != null));

            values.SetValue("age", 7);
            Assert.AreEqual(3, values.ColumnCount);
            Assert.IsTrue(values.Values.All(kvp => kvp.Value != null));

            values.SetValue("name", null);
            Assert.AreEqual(2, values.ColumnCount);
            Assert.IsTrue(values.Values.All(kvp => kvp.Value != null));
        }
    }
}