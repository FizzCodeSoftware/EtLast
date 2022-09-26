namespace FizzCode.EtLast.Tests.Unit;

[TestClass]
public class SplitterAndMergerTests
{
    [TestMethod]
    public void SplitterTestFirstEvaluatorTakesItAll()
    {
        var context = TestExecuter.GetContext();

        var splitter = new Splitter<DefaultRowQueue>(context)
        {
            InputProcess = new EnumerableImporter(context)
            {
                InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            },
        };

        var processes = new ISequence[3];
        for (var i = 0; i < 3; i++)
        {
            processes[i] = new CustomMutator(context)
            {
                Input = splitter,
                Action = row =>
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
            results[i] = processes[i].TakeRowsAndReleaseOwnership(null).ToList();
        }

        Assert.AreEqual(7, results[0].Count);
        Assert.AreEqual(0, results[1].Count);
        Assert.AreEqual(0, results[2].Count);
        Assert.AreEqual(0, processes[0].Pipe.Exceptions.Count);
        Assert.AreEqual(0, processes[1].Pipe.Exceptions.Count);
        Assert.AreEqual(0, processes[2].Pipe.Exceptions.Count);
        Assert.AreNotEqual(processes[0].Pipe, processes[1].Pipe);
        Assert.AreNotEqual(processes[0].Pipe, processes[2].Pipe);
        Assert.AreNotEqual(processes[1].Pipe, processes[2].Pipe);
    }

    [TestMethod]
    public void SplitterTestMultiThread()
    {
        var context = TestExecuter.GetContext();

        var splitter = new Splitter<DefaultRowQueue>(context)
        {
            InputProcess = new EnumerableImporter(context)
            {
                InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            },
        };

        var processes = new ISequence[3];
        for (var i = 0; i < 3; i++)
        {
            processes[i] = new CustomMutator(context)
            {
                Input = splitter,
                Action = row =>
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
                var rows = processes[threadIndex].TakeRowsAndReleaseOwnership(null);
                results[threadIndex] = new List<ISlimRow>(rows);
            });

            thread.Start();
            threads.Add(thread);
        }

        for (var i = 0; i < 3; i++)
        {
            threads[i].Join();
        }

        Assert.AreEqual(7, results[0].Count + results[1].Count + results[2].Count);
        foreach (var p in TestData.Person(context).TakeRowsAndReleaseOwnership(null))
        {
            Assert.IsTrue(
                results[0].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id"))
                || results[1].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id"))
                || results[2].Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
        }

        Assert.AreEqual(0, processes.Sum(x => x.Pipe.Exceptions.Count));
    }

    [TestMethod]
    public void MergerTest()
    {
        var context = TestExecuter.GetContext();

        var merger = new ParallelMerger(context)
        {
            SequenceList = new List<ISequence>(),
        };

        for (var i = 0; i < 3; i++)
        {
            merger.SequenceList.Add(SequenceBuilder.Fluent
                .ReadFrom(TestData.Person(context))
                .CustomCode(new CustomMutator(context)
                {
                    Action = row =>
                    {
                        Thread.Sleep(new Random().Next(100));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                })
                .Build());
        }

        var result = merger.TakeRowsAndReleaseOwnership(null).ToList();
        Assert.AreEqual(21, result.Count);
        foreach (var p in TestData.Person(context).TakeRowsAndReleaseOwnership(null))
        {
            Assert.AreEqual(3, result.Count(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
        }

        Assert.AreEqual(0, merger.SequenceList.Sum(x => x.Pipe.Exceptions.Count));
        Assert.AreEqual(0, merger.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void SplitterTestWithMerger()
    {
        var context = TestExecuter.GetContext();

        var splitter = new Splitter<DefaultRowQueue>(context)
        {
            InputProcess = new EnumerableImporter(context)
            {
                InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            },
        };

        var merger = new ParallelMerger(context)
        {
            SequenceList = new List<ISequence>(),
        };

        for (var i = 0; i < 3; i++)
        {
            merger.SequenceList.Add(SequenceBuilder.Fluent
                .ReadFrom(splitter)
                .CustomCode(new CustomMutator(context)
                {
                    Action = row =>
                    {
                        Thread.Sleep(new Random().Next(10));
                        row["ThreadIndex"] = i;
                        return true;
                    },
                })
                .Build());
        }

        var result = merger.TakeRowsAndReleaseOwnership(null).ToList();
        Assert.AreEqual(7, result.Count);
        foreach (var p in TestData.Person(context).TakeRowsAndReleaseOwnership(null))
        {
            Assert.IsTrue(result.Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
        }

        Assert.AreEqual(0, merger.SequenceList.Sum(x => x.Pipe.Exceptions.Count));
        Assert.AreEqual(0, merger.Pipe.Exceptions.Count);
    }

    [TestMethod]
    public void FluentProcessBuilderTest()
    {
        var context = TestExecuter.GetContext();

        var n = 0;

        var builder = SequenceBuilder.Fluent
            .ImportEnumerable(new EnumerableImporter(context)
            {
                InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            })
            .ProcessOnMultipleThreads(3, (i, mb) => mb
               .CustomCode(new CustomMutator(context)
               {
                   Action = row =>
                   {
                       Thread.Sleep(new Random().Next(10));
                       row["ThreadIndex"] = i;
                       return true;
                   },
               })
               )
            .CustomCode(new CustomMutator(context)
            {
                Action = row =>
                {
                    row["AbsoluteFinalIndex"] = n++;
                    return true;
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        foreach (var p in TestData.Person(context).TakeRowsAndReleaseOwnership(null))
        {
            Assert.IsTrue(result.MutatedRows.Any(m => m.GetAs<int>("id") == p.GetAs<int>("id")));
        }

        Assert.AreEqual(0, result.Process.Pipe.Exceptions.Count);
    }
}
