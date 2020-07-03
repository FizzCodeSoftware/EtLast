namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

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
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddIntAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = ((aggregate.ResultRow.GetAs(column, 0.0d) * aggregate.RowsInGroup) + row.GetAs(column, 0)) / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddLongAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = ((aggregate.ResultRow.GetAs(column, 0.0d) * aggregate.RowsInGroup) + row.GetAs(column, 0L)) / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = ((aggregate.ResultRow.GetAs(column, 0.0d) * aggregate.RowsInGroup) + row.GetAs(column, 0.0d)) / (aggregate.RowsInGroup + 1);
                aggregate.ResultRow.SetValue(column, newValue);
            });
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddAggregator((aggregate, row) =>
            {
                var newValue = ((aggregate.ResultRow.GetAs(column, 0m) * aggregate.RowsInGroup) + row.GetAs(column, 0m)) / (aggregate.RowsInGroup + 1);
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