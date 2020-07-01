namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class MemoryGroupByOperation : AbstractMemoryAggregationOperation
    {
        public delegate object MemoryGroupByOperationDelegate(List<IReadOnlySlimRow> groupRows, string column);
        public Dictionary<string, MemoryGroupByOperationDelegate> ColumnAggregators { get; set; } = new Dictionary<string, MemoryGroupByOperationDelegate>();

        public MemoryGroupByOperation AddColumnAggregator(string column, MemoryGroupByOperationDelegate aggregator)
        {
            ColumnAggregators.Add(column, aggregator);
            return this;
        }

        public override void TransformGroup(List<IReadOnlySlimRow> rows, Func<SlimRow> aggregateCreator)
        {
            var aggregate = aggregateCreator.Invoke();

            foreach (var agg in ColumnAggregators)
            {
                aggregate.SetValue(agg.Key, agg.Value.Invoke(rows, agg.Key));
            }
        }
    }

    public static class GroupByOperationExtensions
    {
        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddIntAverage(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Average(x => x.GetAs(col, 0)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddLongAverage(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Average(x => x.GetAs(col, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleAverage(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Average(x => x.GetAs(col, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalAverage(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Average(x => x.GetAs(col, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntSum(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongSum(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleSum(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalSum(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMax(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Max(x => x.GetAs(col, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMax(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Max(x => x.GetAs(col, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMax(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Max(x => x.GetAs(col, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMax(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Max(x => x.GetAs(col, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMin(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Min(x => x.GetAs(col, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMin(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Min(x => x.GetAs(col, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMin(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Min(x => x.GetAs(col, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMin(this MemoryGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (groupRows, col) => groupRows.Min(x => x.GetAs(col, 0m)));
        }
    }
}