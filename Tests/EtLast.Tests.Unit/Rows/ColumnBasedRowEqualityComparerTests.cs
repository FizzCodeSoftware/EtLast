namespace FizzCode.EtLast.Tests.Unit.Rows
{
    using System.Drawing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ColumnBasedRowEqualityComparerTests
    {
        [TestMethod]
        public void EqualityOnAllColumns()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = "x", };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = "x", };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void EqualityOnSelectedColumns()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = "x", };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = "y", };
            var result = new ColumnBasedRowEqualityComparer() { Columns = new[] { "id" } }.Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void StringUnEqualityOnSelectedColumns()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = "x", };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = "y", };
            var result = new ColumnBasedRowEqualityComparer() { Columns = new[] { "name" } }.Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void StringUnequalityOnAnyColumns()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = "x", };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = "y", };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void EtlRowErrorEquality()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = new EtlRowError("x"), };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = new EtlRowError("x"), };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void EtlRowErrorUnEquality()
        {
            var a = new ValueCollection() { ["id"] = 12, ["name"] = new EtlRowError("x"), };
            var b = new ValueCollection() { ["id"] = 12, ["name"] = new EtlRowError("y"), };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IntegerEquality()
        {
            var a = new ValueCollection() { ["id"] = 12, };
            var b = new ValueCollection() { ["id"] = 12, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IntegerUnEquality()
        {
            var a = new ValueCollection() { ["id"] = 12, };
            var b = new ValueCollection() { ["id"] = 13, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void DoubleEquality()
        {
            var a = new ValueCollection() { ["id"] = -6.5d, };
            var b = new ValueCollection() { ["id"] = -13d / 2d, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void DoubleUnEquality()
        {
            var a = new ValueCollection() { ["id"] = -1d, };
            var b = new ValueCollection() { ["id"] = -1.01d, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void ReferenceEquality()
        {
            var person = new TestData.PersonModel();
            var a = new ValueCollection() { ["person"] = person, };
            var b = new ValueCollection() { ["person"] = person, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void ReferenceUnEquality()
        {
            var a = new ValueCollection() { ["person"] = new TestData.PersonModel(), };
            var b = new ValueCollection() { ["person"] = new TestData.PersonModel(), };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void ColorEquality()
        {
            var a = new ValueCollection() { ["color"] = Color.Red, };
            var b = new ValueCollection() { ["color"] = Color.Red, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void ColorUnEquality()
        {
            var a = new ValueCollection() { ["color"] = Color.Red, };
            var b = new ValueCollection() { ["color"] = Color.Black, };
            var result = new ColumnBasedRowEqualityComparer().Equals(a, b);
            Assert.IsFalse(result);
        }
    }
}