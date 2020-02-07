namespace FizzCode.EtLast.Tests.Unit
{
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OperationProcessTests
    {
        [TestMethod]
        public void KeepOrderTrue()
        {
            var process = CreateKeepOrderProcess(true);
            var rows = process.Evaluate().TakeRowsAndReleaseOwnership();

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

        private static OperationHostProcess CreateKeepOrderProcess(bool keepOrder)
        {
            var context = new EtlContext();

            var process = new OperationHostProcess(context, null, null)
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    KeepOrder = keepOrder,
                    InputBufferSize = 1, // low buffering for strong concurrency
                    MainLoopDelay = 1, // low delay cause returning finished rows almost immediately
                },
                InputProcess = new SeedRowsProcess(context, "SeedRows", null)
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
                Then = (op, row) => Thread.Sleep(50),
            });

            return process;
        }
    }
}