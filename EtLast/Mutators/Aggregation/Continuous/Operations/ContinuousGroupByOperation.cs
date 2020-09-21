namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    public class ContinuousGroupByOperation : AbstractContinuousAggregationOperation
    {
        public delegate void ContinuousGroupByAggregatorDelegate(ContinuousAggregate aggregate, IReadOnlySlimRow row);
        public List<ContinuousGroupByAggregatorDelegate> Aggregators { get; set; } = new List<ContinuousGroupByAggregatorDelegate>();

        public ContinuousGroupByOperation AddAggregator(ContinuousGroupByAggregatorDelegate aggregator)
        {
            Aggregators.Add(aggregator);
            return this;
        }

        public override void TransformAggregate(IReadOnlySlimRow row, ContinuousAggregate aggregate)
        {
            foreach (var aggregator in Aggregators)
            {
                aggregator.Invoke(aggregate, row);
            }
        }
    }

    public static class ContinuousGroupByOperationExtensions
    {
        /// <summary>
        /// New value will be integer.
        /// </summary>
        public static ContinuousGroupByOperation AddIntNumberOfDistinctKeys(this ContinuousGroupByOperation op, string column, RowKeyGenerator keyGenerator)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddIntNumberOfDistinctKeys);
            return op.AddAggregator((aggregate, row) =>
            {
                var key = keyGenerator.Invoke(row);
                if (key != null)
                {
                    var hashset = aggregate.GetStateValue<HashSet<string>>(id, null);
                    if (hashset == null)
                    {
                        hashset = new HashSet<string>();
                        aggregate.SetStateValue(id, hashset);
                    }

                    if (!hashset.Contains(key))
                    {
                        hashset.Add(key);
                        var newValue = hashset.Count;
                        aggregate.ResultRow.SetValue(column, newValue);
                    }
                }
            });
        }

        /// <summary>
        /// New value will be integer.
        /// </summary>
        public static ContinuousGroupByOperation AddIntCount(this ContinuousGroupByOperation op, string targetColumn)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(targetColumn, 0) + 1;
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be integer.
        /// </summary>
        public static ContinuousGroupByOperation AddIntCountWhenNotNull(this ContinuousGroupByOperation op, string targetColumn, string columnToCheckForNull)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                if (row.HasValue(columnToCheckForNull))
                {
                    var newValue = aggregate.ResultRow.GetAs(targetColumn, 0) + 1;
                    aggregate.ResultRow.SetValue(targetColumn, newValue);
                }
            });
        }

        /// <summary>
        /// New value will be integer.
        /// </summary>
        public static ContinuousGroupByOperation AddIntCountWhenNull(this ContinuousGroupByOperation op, string targetColumn, string columnToCheckForNull)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                if (!row.HasValue(columnToCheckForNull))
                {
                    var newValue = aggregate.ResultRow.GetAs(targetColumn, 0) + 1;
                    aggregate.ResultRow.SetValue(targetColumn, newValue);
                }
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddIntAverage(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddIntAverage);

            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0) + row.GetAs(sourceColumn, 0);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (double)(aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddLongAverage(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddLongAverage);

            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0L) + row.GetAs(sourceColumn, 0L);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (double)(aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleAverage(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddDoubleAverage);

            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0.0d) + row.GetAs(sourceColumn, 0.0d);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalAverage(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddDecimalAverage);

            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0m) + row.GetAs(sourceColumn, 0m);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntSum(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(sourceColumn, 0) + row.GetAs(sourceColumn, 0);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongSum(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(sourceColumn, 0L) + row.GetAs(sourceColumn, 0L);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleSum(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(sourceColumn, 0.0d) + row.GetAs(sourceColumn, 0.0d);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalSum(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(sourceColumn, 0m) + row.GetAs(sourceColumn, 0m);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMax(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Max(aggregate.ResultRow.GetAs(sourceColumn, 0), row.GetAs(sourceColumn, 0))
                    : row.GetAs(sourceColumn, 0);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMax(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Max(aggregate.ResultRow.GetAs(sourceColumn, 0L), row.GetAs(sourceColumn, 0L))
                    : row.GetAs(sourceColumn, 0L);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMax(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Max(aggregate.ResultRow.GetAs(sourceColumn, 0.0d), row.GetAs(sourceColumn, 0.0d))
                    : row.GetAs(sourceColumn, 0.0d);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMax(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Max(aggregate.ResultRow.GetAs(sourceColumn, 0m), row.GetAs(sourceColumn, 0m))
                    : row.GetAs(sourceColumn, 0m);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMin(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Min(aggregate.ResultRow.GetAs(sourceColumn, 0), row.GetAs(sourceColumn, 0))
                    : row.GetAs(sourceColumn, 0);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMin(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Min(aggregate.ResultRow.GetAs(sourceColumn, 0L), row.GetAs(sourceColumn, 0L))
                    : row.GetAs(sourceColumn, 0L);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMin(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Min(aggregate.ResultRow.GetAs(sourceColumn, 0.0d), row.GetAs(sourceColumn, 0.0d))
                    : row.GetAs(sourceColumn, 0.0d);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMin(this ContinuousGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            if (targetColumn == null)
                targetColumn = sourceColumn;

            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(sourceColumn)
                    ? Math.Min(aggregate.ResultRow.GetAs(sourceColumn, 0m), row.GetAs(sourceColumn, 0m))
                    : row.GetAs(sourceColumn, 0m);
                aggregate.ResultRow.SetValue(targetColumn, newValue);
            });
        }
    }
}