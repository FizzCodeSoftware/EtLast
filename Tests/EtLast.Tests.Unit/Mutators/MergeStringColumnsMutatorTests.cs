namespace FizzCode.EtLast.Tests.Unit.Mutators
{
    using System;
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class MergeStringColumnsMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<MergeStringColumnsMutator>();
        }

        [TestMethod]
        public void Merge()
        {
            var topic = TestExecuter.GetTopic();
            var builder = ProcessBuilder.Fluent
                .ReadFrom(TestData.Person(topic))
                .MergeStringColumns(new MergeStringColumnsMutator(topic, "merge")
                {
                    ColumnsToMerge = new[] { "name", "eyeColor" },
                    TargetColumn = "Merged",
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["age"] = 17, ["height"] = 160, ["Merged"] = "Abrown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["Merged"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["age"] = 27, ["height"] = 170, ["Merged"] = "Cgreen", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["age"] = 39, ["height"] = 160, ["Merged"] = "Dfake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["Merged"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["Merged"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["Merged"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}