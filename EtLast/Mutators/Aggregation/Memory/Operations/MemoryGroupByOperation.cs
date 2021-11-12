namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public sealed class MemoryGroupByOperation : AbstractMemoryAggregationOperation
    {
        public delegate void MemoryGroupByOperationDelegate(ISlimRow aggregate, List<IReadOnlySlimRow> groupRows);
        public int AggregatorCount => _aggregators.Count;
        private readonly List<MemoryGroupByOperationDelegate> _aggregators = new();

        public MemoryGroupByOperation AddAggregator(MemoryGroupByOperationDelegate aggregator)
        {
            _aggregators.Add(aggregator);
            return this;
        }

        public override void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator)
        {
            var aggregate = aggregateCreator.Invoke();
            foreach (var aggregator in _aggregators)
            {
                aggregator.Invoke(aggregate, groupRows);
            }
        }
    }

    public static class MemoryGroupByOperationExtensions
    {
        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddIntAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Average(x => x.GetAs(sourceColumn, 0)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddLongAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Average(x => x.GetAs(sourceColumn, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Average(x => x.GetAs(sourceColumn, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalAverage(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Average(x => x.GetAs(sourceColumn, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Sum(x => x.GetAs(sourceColumn, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Sum(x => x.GetAs(sourceColumn, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Sum(x => x.GetAs(sourceColumn, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalSum(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Sum(x => x.GetAs(sourceColumn, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Max(x => x.GetAs(sourceColumn, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Max(x => x.GetAs(sourceColumn, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Max(x => x.GetAs(sourceColumn, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMax(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Max(x => x.GetAs(sourceColumn, 0m)));
        }

        /// <summary>
        /// New value will be int.
        /// </summary>
        public static MemoryGroupByOperation AddIntMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Min(x => x.GetAs(sourceColumn, 0)));
        }

        /// <summary>
        /// New value will be long.
        /// </summary>
        public static MemoryGroupByOperation AddLongMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Min(x => x.GetAs(sourceColumn, 0L)));
        }

        /// <summary>
        /// New value will be double.
        /// </summary>
        public static MemoryGroupByOperation AddDoubleMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Min(x => x.GetAs(sourceColumn, 0.0d)));
        }

        /// <summary>
        /// New value will be decimal.
        /// </summary>
        public static MemoryGroupByOperation AddDecimalMin(this MemoryGroupByOperation op, string sourceColumn, string targetColumn = null)
        {
            return op.AddAggregator((aggregate, groupRows) => aggregate[targetColumn ?? sourceColumn] = groupRows.Min(x => x.GetAs(sourceColumn, 0m)));
        }

        public static MemoryGroupByOperation AddLinearRegression(this MemoryGroupByOperation op, string sourceXcolumn, string sourceYcolumn, string alphaColumn, string bColumn, string countColumn)
        {
            return op.AddAggregator((aggregate, groupRows) =>
            {
                var count = 0;
                double sumX = 0.0, sumY = 0.0, sumX2 = 0.0, sumXy = 0.0;
                foreach (var row in groupRows)
                {
                    if (!row.HasValue(sourceXcolumn) || !row.HasValue(sourceYcolumn))
                        return;

                    var x = row.GetAs<double>(sourceXcolumn);
                    var y = row.GetAs<double>(sourceYcolumn);

                    count++;
                    sumX += x;
                    sumX2 += x * x;
                    sumY += y;
                    sumXy += x * y;
                }

                var divider = (count * sumX2) - (sumX * sumX);
                double? alpha;
                double? b = null;

                alpha = count == 1 || Math.Abs(divider) < 0.0001
                    ? 0.0
                    : ((count * sumXy) - (sumX * sumY)) / divider;

                if (count == 1)
                {
                    b = sumY;
                }
                else if (count > 1)
                {
                    var avgX = sumX / count; // need to be the average of the full aggregate group
                    var avgY = sumY / count;
                    b = avgY - (alpha * avgX);
                }

                aggregate[bColumn] = b;
                aggregate[alphaColumn] = alpha;
                aggregate[countColumn] = groupRows.Count;
            });
        }
    }
}