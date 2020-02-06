namespace FizzCode.EtLast.Tests.Unit
{
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class GroupByTests
    {
        protected string[] SampleColumns { get; } = { "id", "name", "age", "height" };

        protected object[][] SampleRows { get; } = {
                new object[] { 0, "A", 7, 160 },
                new object[] { 1, "B", 8, 190 },
                new object[] { 2, "C", 7, 170 },
                new object[] { 3, "A", 9, 160 },
                new object[] { 4, "A", 9, 160 },
                new object[] { 5, "B", 11, 140 },
        };

        [TestMethod]
        public void GroupByName_AverageHeight()
        {
            var context = new EtlContext();

            var groupingColumns = new string[] { "name" };
            var groupByOperation = new GroupByOperation();
            groupByOperation.AddIntAverage("height");

            var process = new AggregationProcess(context, "p1", null)
            {
                GroupingColumns = groupingColumns,
                Operation = groupByOperation,
                InputProcess = new CreateRowsProcess(context, "CreateRows", null)
                {
                    Columns = SampleColumns,
                    InputRows = SampleRows.ToList(),
                },
            };

            var result = process.Evaluate().TakeRows(null).ToList();
            Assert.IsTrue(result.Count == 3);
            Assert.IsTrue(result.Any(x => x.GetAs<string>("name") == "A" && x.GetAs<double>("height") == 160));
            Assert.IsTrue(result.Any(x => x.GetAs<string>("name") == "B" && x.GetAs<double>("height") == 165));
            Assert.IsTrue(result.Any(x => x.GetAs<string>("name") == "C" && x.GetAs<double>("height") == 170));
        }
    }
}