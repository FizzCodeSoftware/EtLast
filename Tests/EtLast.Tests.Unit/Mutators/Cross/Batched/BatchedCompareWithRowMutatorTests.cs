namespace FizzCode.EtLast.Tests.Unit.Mutators.Cross
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class BatchedCompareWithRowMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<BatchedCompareWithRowMutator>();
        }

        [TestMethod]
        public void RemoveWhenMatchAndEquals()
        {
            var context = TestExecuter.GetContext();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .CompareWithRowBatched(new BatchedCompareWithRowMutator(context)
                {
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = rows => TestData.PersonChanged(context),
                        KeyGenerator = row => row.GenerateKey("id"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    EqualityComparer = new ColumnBasedRowEqualityComparer(),
                    MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(5, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveWhenNoMatch()
        {
            var context = TestExecuter.GetContext();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .CompareWithRowBatched(new BatchedCompareWithRowMutator(context)
                {
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = rows => TestData.PersonChanged(context),
                        KeyGenerator = row => row.GenerateKey("id"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    EqualityComparer = new ColumnBasedRowEqualityComparer(),
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveWhenMatchByIdAge()
        {
            var context = TestExecuter.GetContext();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .CompareWithRowBatched(new BatchedCompareWithRowMutator(context)
                {
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = rows => TestData.PersonChanged(context),
                        KeyGenerator = row => row.GenerateKey("id"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    EqualityComparer = new ColumnBasedRowEqualityComparer()
                    {
                        Columns = new string[] { "id", "age" }
                    },
                    MatchAndEqualsAction = new MatchAction(MatchMode.Remove),
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(3, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void MatchButDifferentAction()
        {
            var context = TestExecuter.GetContext();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .CompareWithRowBatched(new BatchedCompareWithRowMutator(context)
                {
                    LookupBuilder = new FilteredRowLookupBuilder()
                    {
                        ProcessCreator = rows => TestData.PersonChanged(context),
                        KeyGenerator = row => row.GenerateKey("id"),
                    },
                    RowKeyGenerator = row => row.GenerateKey("id"),
                    EqualityComparer = new ColumnBasedRowEqualityComparer(),
                    MatchAndEqualsAction = new MatchAction(MatchMode.Custom)
                    {
                        CustomAction = (row, match) => row["compareResult"] = "match+unchanged",
                    },
                    MatchButDifferentAction = new MatchAction(MatchMode.Custom)
                    {
                        CustomAction = (row, match) => row["compareResult"] = "match+diff",
                    },
                    NoMatchAction = new NoMatchAction(MatchMode.Custom)
                    {
                        CustomAction = row => row["compareResult"] = "noMatch",
                    },
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["compareResult"] = "match+diff" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["compareResult"] = "match+unchanged" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["compareResult"] = "match+unchanged" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["compareResult"] = "noMatch" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["compareResult"] = "match+diff" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["compareResult"] = "match+diff" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["compareResult"] = "match+diff" } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}