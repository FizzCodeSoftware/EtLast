namespace FizzCode.EtLast.Tests.Unit.Mutators.Cross
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class JoinMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<JoinMutator>();
        }

        [TestMethod]
        public void NoMatchCustom()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => row.GenerateKey("personId"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = row => row["eyeColor"] = "not found",
                    },
                    ColumnConfiguration = new List<ColumnCopyConfiguration>
                        {
                            new ColumnCopyConfiguration("color", "eyeColor"),
                        }
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(10, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "yellow", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "red", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "green", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "blue" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "yellow" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "black", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "not found", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["eyeColor"] = "not found" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["eyeColor"] = "not found" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["eyeColor"] = "not found" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoMatchRemove()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => row.GenerateKey("personId"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    ColumnConfiguration = new List<ColumnCopyConfiguration>
                        {
                            new ColumnCopyConfiguration("color", "eyeColor"),
                        }
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "yellow", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "red", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "green", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "blue" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "yellow" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "black", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoMatchThrow()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => row.GenerateKey("personId"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    NoMatchAction = new NoMatchAction(MatchMode.Throw),
                    ColumnConfiguration = new List<ColumnCopyConfiguration>
                        {
                            new ColumnCopyConfiguration("color", "eyeColor"),
                        }
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "yellow", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "red", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "green", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "blue" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["eyeColor"] = "yellow" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "black", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionRowKeyGenerator()
        {
            var topic = TestExecuter.GetTopic();
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => { executedRightKeyDelegateCount++; return row.GenerateKey("personId"); },
                    },
                    RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return executedLeftKeyDelegateCount < 3 ? row.GenerateKey("id") : row.GetAs<double>("id").ToString("D", CultureInfo.InvariantCulture); },
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(3, executedLeftKeyDelegateCount);
            Assert.AreEqual(7, executedRightKeyDelegateCount);
            Assert.AreEqual(5, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["color"] = "yellow" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["color"] = "red" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["color"] = "green" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["color"] = "blue" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["color"] = "yellow" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionLookupBuilderKeyGenerator()
        {
            var topic = TestExecuter.GetTopic();
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => { executedRightKeyDelegateCount++; return row.GetAs<double>("personId").ToString("D", CultureInfo.InvariantCulture); },
                    },
                    RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return row.GenerateKey("id"); },
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, executedLeftKeyDelegateCount);
            Assert.AreEqual(1, executedRightKeyDelegateCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionMatchFilter()
        {
            var topic = TestExecuter.GetTopic();
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .Join(new JoinMutator(topic, null)
                {
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = TestData.PersonEyeColor(topic),
                        KeyGenerator = row => { executedRightKeyDelegateCount++; return row.GenerateKey("personId"); },
                    },
                    RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return row.GenerateKey("id"); },
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    MatchFilter = match => match.GetAs<double>("id") == 7,
                    ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, executedLeftKeyDelegateCount);
            Assert.AreEqual(7, executedRightKeyDelegateCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }
    }
}