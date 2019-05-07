﻿namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractAggregationOperation : IAggregationOperation
    {
        public string Name { get; set; }
        public string InstanceName { get; set; } // todo: update name when InstanceName is set
        public int Index { get; private set; }
        public IProcess Process { get; private set; }

        protected AbstractAggregationOperation()
        {
            Name = GetType().Name;
        }

        public abstract IEnumerable<IRow> TransformGroup(string[] groupingColumns, IProcess process, List<IRow> rows);

        public void SetParent(IProcess process, int index)
        {
            Process = process;
            Index = index;
        }

        public void Prepare()
        {
        }

        public void Shutdown()
        {
        }
    }
}