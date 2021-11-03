namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class MemoryGroupByOperation : AbstractMemoryAggregationOperation
    {
        public delegate object MemoryGroupByOperationDelegate(List<IReadOnlySlimRow> groupRows, string sourceColumn);

        private readonly List<AggregatorInfo> _columnAggregators = new();

        public MemoryGroupByOperation AddColumnAggregator(MemoryGroupByOperationDelegate aggregator, string sourceColumn, string targetColumn = null)
        {
            _columnAggregators.Add(new AggregatorInfo()
            {
                Aggregator = aggregator,
                SourceColumn = sourceColumn,
                TargetColumn = targetColumn ?? sourceColumn,
            });

            return this;
        }

        public override void TransformGroup(List<IReadOnlySlimRow> rows, Func<ISlimRow> aggregateCreator)
        {
            var aggregate = aggregateCreator.Invoke();
            foreach (var aggregatorInfo in _columnAggregators)
            {
                aggregate[aggregatorInfo.TargetColumn] = aggregatorInfo.Aggregator.Invoke(rows, aggregatorInfo.SourceColumn);
            }
        }

        private class AggregatorInfo
        {
            public MemoryGroupByOperationDelegate Aggregator { get; set; }
            public string SourceColumn { get; set; }
            public string TargetColumn { get; set; }
        }
    }

    public static class MemoryGroupByOperationExtensions
    {
        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddIntAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Average(x => x.GetAs(col, 0)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddLongAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Average(x => x.GetAs(col, 0L)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Average(x => x.GetAs(col, 0.0d)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Average(x => x.GetAs(col, 0m)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0L)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0.0d)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Sum(x => x.GetAs(col, 0m)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Max(x => x.GetAs(col, 0)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Max(x => x.GetAs(col, 0L)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Max(x => x.GetAs(col, 0.0d)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Max(x => x.GetAs(col, 0m)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Min(x => x.GetAs(col, 0)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Min(x => x.GetAs(col, 0L)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Min(x => x.GetAs(col, 0.0d)), sourceColumn, targetColumn);
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddColumnAggregator((groupRows, col) => groupRows.Min(x => x.GetAs(col, 0m)), sourceColumn, targetColumn);
        }
    }
}