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
        public static ContinuousGroupByOperation AddIntCountWhenNotNull(this ContinuousGroupByOperation op, string targetColumn, string columnToCheckForValue)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                if (row.HasValue(columnToCheckForValue))
                {
                    var newValue = aggregate.ResultRow.GetAs(targetColumn, 0) + 1;
                    aggregate.ResultRow.SetValue(targetColumn, newValue);
                }
            });
        }

        /// <summary>
        /// New value will be integer.
        /// </summary>
        public static ContinuousGroupByOperation AddIntCountWhenNull(this ContinuousGroupByOperation op, string columnWithCount, string columnToCheckForValue)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                if (!row.HasValue(columnToCheckForValue))
                {
                    var newValue = aggregate.ResultRow.GetAs(columnWithCount, 0) + 1;
                    aggregate.ResultRow.SetValue(columnWithCount, newValue);
                }
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddIntAverage(this ContinuousGroupByOperation op, string column)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddIntAverage);
            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0) + row.GetAs(column, 0);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (double)(aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddLongAverage(this ContinuousGroupByOperation op, string column)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddLongAverage);
            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0L) + row.GetAs(column, 0L);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (double)(aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleAverage(this ContinuousGroupByOperation op, string column)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddDoubleAverage);
            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0.0d) + row.GetAs(column, 0.0d);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalAverage(this ContinuousGroupByOperation op, string column)
        {
            var id = op.Aggregators.Count.ToString("D", CultureInfo.InvariantCulture) + ":" + nameof(AddDecimalAverage);
            return op.AddAggregator((aggregate, row) =>
            {
                var newTotal = aggregate.GetStateValue(id, 0m) + row.GetAs(column, 0m);
                aggregate.SetStateValue(id, newTotal);

                var newValue = newTotal / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(column, 0) + row.GetAs(column, 0);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(column, 0L) + row.GetAs(column, 0L);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(column, 0.0d) + row.GetAs(column, 0.0d);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.GetAs(column, 0m) + row.GetAs(column, 0m);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Max(aggregate.ResultRow.GetAs(column, 0), row.GetAs(column, 0))
                    : row.GetAs(column, 0);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Max(aggregate.ResultRow.GetAs(column, 0L), row.GetAs(column, 0L))
                    : row.GetAs(column, 0L);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Max(aggregate.ResultRow.GetAs(column, 0.0d), row.GetAs(column, 0.0d))
                    : row.GetAs(column, 0.0d);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Max(aggregate.ResultRow.GetAs(column, 0m), row.GetAs(column, 0m))
                    : row.GetAs(column, 0m);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Min(aggregate.ResultRow.GetAs(column, 0), row.GetAs(column, 0))
                    : row.GetAs(column, 0);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Min(aggregate.ResultRow.GetAs(column, 0L), row.GetAs(column, 0L))
                    : row.GetAs(column, 0L);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Min(aggregate.ResultRow.GetAs(column, 0.0d), row.GetAs(column, 0.0d))
                    : row.GetAs(column, 0.0d);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = aggregate.ResultRow.HasValue(column)
                    ? Math.Min(aggregate.ResultRow.GetAs(column, 0m), row.GetAs(column, 0m))
                    : row.GetAs(column, 0m);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }
    }
}