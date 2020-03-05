namespace FizzCode.EtLast.Tests.Unit.Rows
{
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CountableOnlyRowLookupTests
    {
        [TestMethod]
        public void NotNullIdentity()
        {
            var topic = TestExecuter.GetTopic();
            var lookup = new CountableOnlyRowLookup();
            var builder = new RowLookupBuilder()
            {
                Process = TestData.Person(topic),
                KeyGenerator = row => row.GenerateKey("id"),
            };

            builder.Append(lookup, null);
            Assert.AreEqual(7, lookup.Keys.Count());
            Assert.AreEqual(7, lookup.Count);
            Assert.AreEqual(1, lookup.GetRowCountByKey("0"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("1"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("2"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("3"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("4"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("5"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("6"));
        }

        [TestMethod]
        public void NullableIdentity()
        {
            var topic = TestExecuter.GetTopic();
            var lookup = new CountableOnlyRowLookup();
            var builder = new RowLookupBuilder()
            {
                Process = TestData.Person(topic),
                KeyGenerator = row => row.GenerateKey("age"),
            };

            builder.Append(lookup, null);
            Assert.AreEqual(6, lookup.Keys.Count());
            Assert.AreEqual(6, lookup.Count);
            Assert.AreEqual(1, lookup.GetRowCountByKey("17"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("8"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("27"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("39"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("-3"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("11"));
            Assert.AreEqual(0, lookup.GetRowCountByKey(null));
        }

        [TestMethod]
        public void NullableMulti()
        {
            var topic = TestExecuter.GetTopic();
            var lookup = new CountableOnlyRowLookup();
            var builder = new RowLookupBuilder()
            {
                Process = TestData.Person(topic),
                KeyGenerator = row => row.GenerateKey("name"),
            };

            builder.Append(lookup, null);
            Assert.AreEqual(6, lookup.Keys.Count());
            Assert.AreEqual(2, lookup.GetRowCountByKey("A"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("B"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("C"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("D"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("E"));
            Assert.AreEqual(1, lookup.GetRowCountByKey("fake"));
        }
    }
}