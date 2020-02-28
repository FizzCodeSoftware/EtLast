﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ContinuousGroupByOperation : AbstractContinuousAggregationOperation
    {
        public Dictionary<string, Func<IRow, int, IRow, string, object>> ColumnAggregators { get; set; } = new Dictionary<string, Func<IRow, int, IRow, string, object>>();

        public ContinuousGroupByOperation AddColumnAggregator(string column, Func<IRow, int, IRow, string, object> aggregator)
        {
            ColumnAggregators.Add(column, aggregator);
            return this;
        }

        public override void TransformGroup(string[] groupingColumns, IRow row, IRow aggregateRow, int rowsInGroup)
        {
            foreach (var kvp in ColumnAggregators)
            {
                var column = kvp.Key;
                var aggregatedValue = kvp.Value.Invoke(aggregateRow, rowsInGroup, row, column);
                aggregateRow.SetStagedValue(column, aggregatedValue);
            }

            aggregateRow.ApplyStaging();
        }
    }

    public static class ContinuousGroupByOperationExtensions
    {
        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddIntAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => ((aggregateRow.GetAs(col, 0.0d) * rowsInGroup) + row.GetAs(col, 0)) / (rowsInGroup + 1));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddLongAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => ((aggregateRow.GetAs(col, 0.0d) * rowsInGroup) + row.GetAs(col, 0L)) / (rowsInGroup + 1));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => ((aggregateRow.GetAs(col, 0.0d) * rowsInGroup) + row.GetAs(col, 0.0d)) / (rowsInGroup + 1));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalAverage(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => ((aggregateRow.GetAs(col, 0m) * rowsInGroup) + row.GetAs(col, 0m)) / (rowsInGroup + 1));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.GetAs(col, 0) + row.GetAs(col, 0));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.GetAs(col, 0L) + row.GetAs(col, 0L));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.GetAs(col, 0.0d) + row.GetAs(col, 0.0d));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalSum(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.GetAs(col, 0m) + row.GetAs(col, 0m));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Max(aggregateRow.GetAs(col, 0), row.GetAs(col, 0))
                : row.GetAs(col, 0));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Max(aggregateRow.GetAs(col, 0L), row.GetAs(col, 0L))
                : row.GetAs(col, 0L));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Max(aggregateRow.GetAs(col, 0.0d), row.GetAs(col, 0.0d))
                : row.GetAs(col, 0.0d));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMax(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Max(aggregateRow.GetAs(col, 0m), row.GetAs(col, 0m))
                : row.GetAs(col, 0m));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static ContinuousGroupByOperation AddIntMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Min(aggregateRow.GetAs(col, 0), row.GetAs(col, 0))
                : row.GetAs(col, 0));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static ContinuousGroupByOperation AddLongMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Min(aggregateRow.GetAs(col, 0L), row.GetAs(col, 0L))
                : row.GetAs(col, 0L));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static ContinuousGroupByOperation AddDoubleMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Min(aggregateRow.GetAs(col, 0.0d), row.GetAs(col, 0.0d))
                : row.GetAs(col, 0.0d));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static ContinuousGroupByOperation AddDecimalMin(this ContinuousGroupByOperation op, string column)
        {
            return op.AddColumnAggregator(column, (aggregateRow, rowsInGroup, row, col) => aggregateRow.HasValue(col)
                ? Math.Min(aggregateRow.GetAs(col, 0m), row.GetAs(col, 0m))
                : row.GetAs(col, 0m));
        }
    }
}