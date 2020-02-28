namespace FizzCode.EtLast.Tests.Unit
{
    using System.Collections.Generic;
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class BatchedJoinMutatorTests
    {
        [TestMethod]
        public void ThrowsInvalidProcessParameterException()
        {
            Assert.That.ThrowsInvalidProcessParameterException<BatchedJoinMutator>();
        }

        [TestMethod]
        public void NoMatchCustom()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 4,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Custom)
                        {
                            CustomAction = (proc, row) => row.SetValue("eyeColor", "not found"),
                        },
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, executedBatchCount);
            Assert.AreEqual(10, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "yellow", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "red", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "green", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "blue", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "yellow", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["color"] = "black", ["eyeColor"] = "green" },
                new Dictionary<string, object>() { ["id"] = 3, ["name"] = "D", ["color"] = null, ["eyeColor"] = "not found" },
                new Dictionary<string, object>() { ["id"] = 4, ["name"] = "E", ["color"] = null, ["eyeColor"] = "not found" },
                new Dictionary<string, object>() { ["id"] = 5, ["name"] = "A", ["color"] = null, ["eyeColor"] = "not found" },
                new Dictionary<string, object>() { ["id"] = 6, ["name"] = "fake", ["color"] = null, ["eyeColor"] = "not found" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoMatchRemove()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 4,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, executedBatchCount);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "yellow", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "red", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "green", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "blue", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "yellow", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["color"] = "black", ["eyeColor"] = "green" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void NoMatchThrow1()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 1,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Throw),
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(4, executedBatchCount);
            Assert.AreEqual(6, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "yellow", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "red", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "green", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "blue", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 1, ["name"] = "B", ["color"] = "yellow", ["eyeColor"] = null },
                new Dictionary<string, object>() { ["id"] = 2, ["name"] = "C", ["color"] = "black", ["eyeColor"] = "green" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void NoMatchThrow4()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 4,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Throw),
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, executedBatchCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionRowKeyGenerator()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 2,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => { executedRightKeyDelegateCount++; return row.GenerateKey("personId"); },
                        },
                        RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return executedLeftKeyDelegateCount < 3 ? row.GenerateKey("id") : row.GetAs<double>("id").ToString("D", CultureInfo.InvariantCulture); },
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, executedBatchCount);
            Assert.AreEqual(3, executedLeftKeyDelegateCount);
            Assert.AreEqual(7, executedRightKeyDelegateCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionLookupBuilderKeyGenerator()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 1,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => { executedRightKeyDelegateCount++; return executedBatchCount < 2 ? row.GenerateKey("personId") : row.GetAs<double>("personId").ToString("D", CultureInfo.InvariantCulture); },
                        },
                        RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return row.GenerateKey("id"); },
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(2, executedBatchCount);
            Assert.AreEqual(3, executedLeftKeyDelegateCount);
            Assert.AreEqual(8, executedRightKeyDelegateCount);
            Assert.AreEqual(3, result.MutatedRows.Count);
            Assert.That.OrderedMatch(result, new List<Dictionary<string, object>>() {
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "yellow", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "red", ["eyeColor"] = "brown" },
                new Dictionary<string, object>() { ["id"] = 0, ["name"] = "A", ["color"] = "green", ["eyeColor"] = "brown" } });
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }

        [TestMethod]
        public void DelegateThrowsExceptionMatchFilter()
        {
            var topic = TestExecuter.GetTopic();
            var executedBatchCount = 0;
            var executedLeftKeyDelegateCount = 0;
            var executedRightKeyDelegateCount = 0;
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new BatchedJoinMutator(topic, null)
                    {
                        BatchSize = 1,
                        LookupBuilder = new FilteredRowLookupBuilder()
                        {
                            ProcessCreator = filterRows =>
                            {
                                executedBatchCount++;
                                return TestData.PersonEyeColor(topic);
                            },
                            KeyGenerator = row => { executedRightKeyDelegateCount++; return row.GenerateKey("personId"); },
                        },
                        RowKeyGenerator = row => { executedLeftKeyDelegateCount++; return row.GenerateKey("id"); },
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        MatchFilter = match => match.GetAs<double>("id") == 7,
                        ColumnConfiguration = ColumnCopyConfiguration.StraightCopy("color"),
                    }
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(1, executedBatchCount);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is ProcessExecutionException);
        }
    }
}