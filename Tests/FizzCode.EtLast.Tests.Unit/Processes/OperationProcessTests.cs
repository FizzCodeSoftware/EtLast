namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Threading;

    [TestClass]
    public class OperationProcessTests
    {
        [TestMethod]
        public void KeepOrderTrue()
        {
            var process = CreateKeepOrderProcess(true);
            var rows = process.Evaluate();

            IRow prevRow = null;
            foreach (var row in rows)
            {
                if (prevRow != null)
                {
                    Assert.IsTrue(prevRow.GetAs<int>("id") < row.GetAs<int>("id"));
                }

                prevRow = row;
            }
        }

        [TestMethod]
        public void KeepOrderFalse()
        {
            var process = CreateKeepOrderProcess(false);
            var rows = process.Evaluate();

            var allInOrder = true;
            IRow prevRow = null;
            foreach (var row in rows)
            {
                if (prevRow != null)
                {
                    if (prevRow.GetAs<int>("id") >= row.GetAs<int>("id"))
                    {
                        allInOrder = false;
                    }
                }

                prevRow = row;
            }

            Assert.IsFalse(allInOrder);
        }

        private static OperationProcess CreateKeepOrderProcess(bool keepOrder)
        {
            var context = new EtlContext<DictionaryRow>();

            var process = new OperationProcess(context)
            {
                Configuration = new OperationProcessConfiguration()
                {
                    WorkerType = typeof(BalancedInProcessWorker), // not allowing batching which reduces re-queueing
                    KeepOrder = keepOrder,
                    WorkerCount = 4, // high amount of workers for strong concurrency
                    InputBufferSize = 1, // low buffering for strong concurrency
                    MainLoopDelay = 1, // low delay cause returning finished rows almost immediately
                },
                InputProcess = new SeedRowsProcess(context, "SeedRows")
                {
                    Columns = new[] { "id", "name" },
                    Count = 1000,
                },
            };
            process.AddOperation(new CustomOperation() { Then = (op, row) => row.SetValue("c1", 1, op) });
            process.AddOperation(new CustomOperation() { Then = (op, row) => row.SetValue("c2", 2, op) });
            process.AddOperation(new CustomOperation() { Then = (op, row) => row.SetValue("c3", 3, op) });
            process.AddOperation(new CustomOperation() { Then = (op, row) => row.SetValue("c4", 4, op) });
            process.AddOperation(new CustomOperation() { Then = (op, row) => row.SetValue("c5", 5, op) });
            process.AddOperation(new CustomOperation()
            {
                If = row => row.GetAs<int>("id") % 100 == 0,

                // this must be way higher than MainLoopDelay so this row won't be returned in order
                Then = (op, row) =>
                {
                    Thread.Sleep(50);
                },
            });

            return process;
        }
    }
}