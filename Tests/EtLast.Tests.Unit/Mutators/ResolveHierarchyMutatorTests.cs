namespace FizzCode.EtLast.Tests.Unit.Mutators
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ResolveHierarchyMutatorTests
    {
        [TestMethod]
        public void KeepOriginalLevelColumns()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(context),
                Mutators = new MutatorList()
                {
                    new ResolveHierarchyMutator(context, null, null)
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["code"] = "A", ["level1"] = "AAA", ["level"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["code"] = "B", ["level2"] = "BBB", ["parentId"] = 0, ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["code"] = "C", ["level3"] = "CCC", ["parentId"] = 1, ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["code"] = "D", ["level3"] = "DDD", ["parentId"] = 1, ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["code"] = "E", ["level2"] = "EEE", ["parentId"] = 0, ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["code"] = "F", ["level2"] = "FFF", ["parentId"] = 0, ["level"] = 1 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void RemoveLevelColumns()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(context),
                Mutators = new MutatorList()
                {
                    new ResolveHierarchyMutator(context, null, null)
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["code"] = "A", ["level"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["code"] = "B", ["parentId"] = 0, ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["code"] = "C", ["parentId"] = 1, ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["code"] = "D", ["parentId"] = 1, ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["code"] = "E", ["parentId"] = 0, ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["code"] = "F", ["parentId"] = 0, ["level"] = 1 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoNewLevelColumn()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(context),
                Mutators = new MutatorList()
                {
                    new ResolveHierarchyMutator(context, null, null)
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["code"] = "A" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["code"] = "B", ["parentId"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["code"] = "C", ["parentId"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["code"] = "D", ["parentId"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["code"] = "E", ["parentId"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["code"] = "F", ["parentId"] = 0 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NewNameColumn()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(context),
                Mutators = new MutatorList()
                {
                    new ResolveHierarchyMutator(context, null, null)
                    {
                        IdentityColumn = "id",
                        NewColumnWithParentId = "parentId",
                        NewColumnWithLevel = "level",
                        NewColumnWithName = "name",
                        LevelColumns = new[] { "level1", "level2", "level3" },
                        RemoveLevelColumns = true,
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["code"] = "A", ["level"] = 0, ["name"] = "AAA" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["code"] = "B", ["parentId"] = 0, ["level"] = 1, ["name"] = "BBB" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["code"] = "C", ["parentId"] = 1, ["level"] = 2, ["name"] = "CCC" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["code"] = "D", ["parentId"] = 1, ["level"] = 2, ["name"] = "DDD" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["code"] = "E", ["parentId"] = 0, ["level"] = 1, ["name"] = "EEE" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["code"] = "F", ["parentId"] = 0, ["level"] = 1, ["name"] = "FFF" } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void IdentityColumnIsString()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.RoleHierarchy(context),
                Mutators = new MutatorList()
                {
                    new InPlaceConvertMutator(context, null, "ConvertIdToString")
                    {
                        Columns = new[] {"id" },
                        TypeConverter = new StringConverter(),
                    },
                    new ResolveHierarchyMutator(context, null, null)
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
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "0", ["code"] = "A", ["level1"] = "AAA", ["level"] = 0 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "1", ["code"] = "B", ["level2"] = "BBB", ["parentId"] = "0", ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "2", ["code"] = "C", ["level3"] = "CCC", ["parentId"] = "1", ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "3", ["code"] = "D", ["level3"] = "DDD", ["parentId"] = "1", ["level"] = 2 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "4", ["code"] = "E", ["level2"] = "EEE", ["parentId"] = "0", ["level"] = 1 },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = "5", ["code"] = "F", ["level2"] = "FFF", ["parentId"] = "0", ["level"] = 1 } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}