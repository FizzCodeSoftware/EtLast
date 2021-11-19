namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SplitterAndMergerTests
    {
        [TestMethod]
        public void SplitterTestFirstEvaluatorTakesItAll()
        {
            var context = TestExecuter.GetContext();

            var splitter = new Splitter<DefaultRowQueue>(context, null, null)
            {
                InputProcess = new EnumerableImporter(context, null, null)
                {
                    InputGenerator = caller => TestData.Person(context).Evaluate(caller).TakeRowsAndReleaseOwnership(),
                },
            };

            var processes = new IProducer[4];
            for (var i = 0; i < 3; i++)
            {
                processes[i] = new CustomMutator(context, null, null)
                {
                    InputProcess = splitter,
                    Then = row =>
                    {
                        Thread.Sleep(new Random().Next(10));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                };
            }

            var results = new List<ISlimRow>[3];
            for (var i = 0; i < 3; i++)
            {
                results[i] = processes[i].Evaluate().TakeRowsAndReleaseOwnership().ToList();
            }

            Assert.AreEqual(7, results[0].Count);
            Assert.AreEqual(0, results[1].Count);
            Assert.AreEqual(0, results[2].Count);
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void SplitterTestMultiThread()
        {
            var context = TestExecuter.GetContext();

            var splitter = new Splitter<DefaultRowQueue>(context, null, null)
            {
                InputProcess = new EnumerableImporter(context, null, null)
                {
                    InputGenerator = caller => TestData.Person(context).Evaluate(caller).TakeRowsAndReleaseOwnership(),
                },
            };

            var processes = new IProducer[4];
            for (var i = 0; i < 3; i++)
            {
                processes[i] = new CustomMutator(context, null, null)
                {
                    InputProcess = splitter,
                    Then = row =>
                    {
                        Thread.Sleep(new Random().Next(10));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                };
            }

            var threads = new List<Thread>();
            var results = new List<ISlimRow>[3];

            for (var i = 0; i < 3; i++)
            {
                var threadIndex = i;
                var thread = new Thread(() =>
                {
                    results[threadIndex] = new List<ISlimRow>();
                    foreach (var row in processes[threadIndex].Evaluate().TakeRowsAndReleaseOwnership())
                    {
                        results[threadIndex].Add(row);
                    }
                });

                thread.Start();
                threads.Add(thread);
            }

            for (var i = 0; i < 3; i++)
            {
                threads[i].Join();
            }

            Assert.AreEqual(7, results[0].Count + results[1].Count + results[2].Count);
            foreach (var p in TestData.Person(context).Evaluate().TakeRowsAndReleaseOwnership())
            {
                Assert.IsTrue(
                    results[0].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id"))
                    || results[1].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id"))
                    || results[2].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
            }

            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void MergerTest()
        {
            var context = TestExecuter.GetContext();

            var merger = new ParallelMerger(context, null, null)
            {
                ProcessList = new List<IProducer>(),
            };

            for (var i = 0; i < 3; i++)
            {
                merger.ProcessList.Add(new CustomMutator(context, null, null)
                {
                    InputProcess = TestData.Person(context),
                    Then = row =>
                    {
                        Thread.Sleep(new Random().Next(100));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                });
            }

            var result = merger.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(21, result.Count);
            foreach (var p in TestData.Person(context).Evaluate().TakeRowsAndReleaseOwnership())
            {
                Assert.AreEqual(3, result.Count(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
            }

            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void SplitterTestWithMerger()
        {
            var context = TestExecuter.GetContext();

            var splitter = new Splitter<DefaultRowQueue>(context, null, null)
            {
                InputProcess = new EnumerableImporter(context, null, null)
                {
                    InputGenerator = caller => TestData.Person(context).Evaluate(caller).TakeRowsAndReleaseOwnership(),
                },
            };

            var merger = new ParallelMerger(context, null, null)
            {
                ProcessList = new List<IProducer>(),
            };

            for (var i = 0; i < 3; i++)
            {
                merger.ProcessList.Add(new CustomMutator(context, null, null)
                {
                    InputProcess = splitter,
                    Then = row =>
                    {
                        Thread.Sleep(new Random().Next(10));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                });
            }

            var result = merger.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            Assert.AreEqual(7, result.Count);
            foreach (var p in TestData.Person(context).Evaluate().TakeRowsAndReleaseOwnership())
            {
                Assert.IsTrue(result.Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
            }

            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }

        [TestMethod]
        public void FluentProcessBuilderTest()
        {
            var context = TestExecuter.GetContext();

            var n = 0;

            var builder = ProcessBuilder.Fluent
                .ImportEnumerable(new EnumerableImporter(context, null, null)
                {
                    InputGenerator = caller => TestData.Person(context).Evaluate(caller).TakeRowsAndReleaseOwnership(),
                })
                .ProcessOnMultipleThreads(context, null, 3, (i, mb) => mb
                   .CustomCode(new CustomMutator(context, null, null)
                   {
                       Then = row =>
                       {
                           Thread.Sleep(new Random().Next(10));
                           row["ThreadIndex"] = i;
                           return true;
                       },
                   })
                   )
                .CustomCode(new CustomMutator(context, null, null)
                {
                    Then = row =>
                    {
                        row["AbsoluteFinalIndex"] = n++;
                        return true;
                    },
                });

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            foreach (var p in TestData.Person(context).Evaluate().TakeRowsAndReleaseOwnership())
            {
                Assert.IsTrue(result.MutatedRows.Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
            }

            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
        }
    }
}