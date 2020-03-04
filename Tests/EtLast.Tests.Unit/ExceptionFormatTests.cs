namespace FizzCode.EtLast.Tests.Unit.Exceptions
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ExceptionFormatTests
    {
        [TestMethod]
        public void RowToDebugStringStartsWithUid()
        {
            var topic = TestExecuter.GetTopic();
            var row = topic.Context.CreateRow(null, new ValueCollection());

            var debugString = row.ToDebugString();
            Assert.IsTrue(debugString.StartsWith("uid", StringComparison.InvariantCultureIgnoreCase));
        }

        [TestMethod]
        public void DummyForDevelopment1()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, "MyBrokenMutator")
                    {
                        Then = (proc, row) =>
                        {
                            throw new Exception("ohh");
                        },
                    },
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);
            Debug.WriteLine(msg);
            Debugger.Break();
        }

        [TestMethod]
        public void DummyForDevelopment2()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, "MyBrokenMutator"),
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);
            Debug.WriteLine(msg);
            Debugger.Break();
        }

        [TestMethod]
        public void DummyForDevelopment3()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new JoinMutator(topic, "MyBrokenMutator")
                    {
                        LookupBuilder = new RowLookupBuilder()
                        {
                            Process = TestData.PersonEyeColor(topic),
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Throw),
                        ColumnConfiguration = new List<ColumnCopyConfiguration>(),
                    }
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);
            Debug.WriteLine(msg);
            Debugger.Break();
        }

        [TestMethod]
        public void DummyForDevelopment4()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new JoinMutator(topic, "MyBrokenMutator")
                    {
                        LookupBuilder = new RowLookupBuilder()
                        {
                            Process = TestData.PersonEyeColor(topic),
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        NoMatchAction = new NoMatchAction(MatchMode.Custom)
                        {
                             CustomAction = (proc, row) =>
                             {
                                 throw new Exception("ohh");
                             },
                        },
                        ColumnConfiguration = new List<ColumnCopyConfiguration>(),
                    }
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);
            Debug.WriteLine(msg);
            Debugger.Break();
        }

        [TestMethod]
        public void DummyForDevelopment5()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new JoinMutator(topic, "MyBrokenMutator")
                    {
                        LookupBuilder = new RowLookupBuilder()
                        {
                            Process = TestData.PersonEyeColor(topic),
                            KeyGenerator = row => row.GenerateKey("personId"),
                        },
                        RowKeyGenerator = row => row.GenerateKey("id"),
                        MatchCustomAction = (proc, row, match) =>
                        {
                            throw new Exception("ohh");
                        },
                        ColumnConfiguration = new List<ColumnCopyConfiguration>(),
                    }
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);
            Debug.WriteLine(msg);
            Debugger.Break();
        }
    }
}