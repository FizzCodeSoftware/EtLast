namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class HierarchyParentIdCalculatorMutatorTests
    {
        protected string[] SampleColumns { get; } = { "id", "name", "level1", "level2", "level3" };

        protected object[][] SampleRows { get; } = {
                new object[] { 0, "A", "AAA" },
                new object[] { 1, "B", null, "BBB" },
                new object[] { 2, "C", null, null, "CCC" },
                new object[] { 3, "D", null, null, "DDD" },
                new object[] { 4, "E", null, "EEE" },
                new object[] { 5, "F", null, "FFF" },
        };

        [TestMethod]
        public void KeepOriginalLevelColumns()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(topic),
                Mutators = new MutatorList()
                {
                    new HierarchyParentIdCalculatorMutator(topic, null)
                    {
                        IdentityColumn = "id",
                        NewColumnWithParentId = "parentId",
                        NewColumnWithLevel = "level",
                        LevelColumns = new[] { "level1", "level2", "level3" },
                        RemoveLevelColumns = false,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["level"] = 0, ["parentId"] = null, ["level1"] = "AAA", ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = "BBB", ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["level"] = 2, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = "CCC" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["level"] = 2, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = "DDD" },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = "EEE", ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "F", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = "FFF", ["level3"] = null } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveLevelColumns()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(topic),
                Mutators = new MutatorList()
                {
                    new HierarchyParentIdCalculatorMutator(topic, null)
                    {
                        IdentityColumn = "id",
                        NewColumnWithParentId = "parentId",
                        NewColumnWithLevel = "level",
                        LevelColumns = new[] { "level1", "level2", "level3" },
                        RemoveLevelColumns = true,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["level"] = 0, ["parentId"] = null, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["level"] = 2, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["level"] = 2, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "F", ["level"] = 1, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoNewLevelColumn()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(topic),
                Mutators = new MutatorList()
                {
                    new HierarchyParentIdCalculatorMutator(topic, null)
                    {
                        IdentityColumn = "id",
                        NewColumnWithParentId = "parentId",
                        LevelColumns = new[] { "level1", "level2", "level3" },
                        RemoveLevelColumns = true,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["level"] = null, ["parentId"] = null, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["level"] = null, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["level"] = null, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["level"] = null, ["parentId"] = 1, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E", ["level"] = null, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "F", ["level"] = null, ["parentId"] = 0, ["level1"] = null, ["level2"] = null, ["level3"] = null } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IdentityColumnIsString()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(topic),
                Mutators = new MutatorList()
                {
                    new InPlaceConvertMutator(topic, "ConvertIdToString")
                    {
                        Columns = new[] {"id" },
                        TypeConverter = new StringConverter(),
                    },
                    new HierarchyParentIdCalculatorMutator(topic, null)
                    {
                        IdentityColumn = "id",
                        NewColumnWithParentId = "parentId",
                        NewColumnWithLevel = "level",
                        LevelColumns = new[] { "level1", "level2", "level3" },
                        RemoveLevelColumns = false,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = "0", ["name"] = "A", ["level"] = 0, ["parentId"] = null, ["level1"] = "AAA", ["level2"] = null, ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = "1", ["name"] = "B", ["level"] = 1, ["parentId"] = "0", ["level1"] = null, ["level2"] = "BBB", ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = "2", ["name"] = "C", ["level"] = 2, ["parentId"] = "1", ["level1"] = null, ["level2"] = null, ["level3"] = "CCC" },
                new Dictionary<string, object>() { ["id"] = "3", ["name"] = "D", ["level"] = 2, ["parentId"] = "1", ["level1"] = null, ["level2"] = null, ["level3"] = "DDD" },
                new Dictionary<string, object>() { ["id"] = "4", ["name"] = "E", ["level"] = 1, ["parentId"] = "0", ["level1"] = null, ["level2"] = "EEE", ["level3"] = null },
                new Dictionary<string, object>() { ["id"] = "5", ["name"] = "F", ["level"] = 1, ["parentId"] = "0", ["level1"] = null, ["level2"] = "FFF", ["level3"] = null } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}