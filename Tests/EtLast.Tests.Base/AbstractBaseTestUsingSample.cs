namespace FizzCode.EtLast.Tests.Base
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractBaseTestUsingSample
    {
        protected string[] SampleColumns { get; } = { "id", "name", "age", "fkid", "date", "time", "datetime" };

        protected object[][] SampleRows { get; } = {
                new object[] { 0, "A", 1, "7", new DateTime(2018,1,1), new TimeSpan(8,0,0), new DateTime(2018,2,11,12,0,0) },
                new object[] { 1, "B", 2, null } };

        public IOperationProcess CreateProcess()
        {
            var context = new EtlContext<DictionaryRow>();

            return new OperationProcess(context)
            {
                Configuration = new OperationProcessConfiguration()
                {
                    WorkerCount = 2,
                    MainLoopDelay = 10,
                    InputBufferSize = 10,
                }
            };
        }

        public List<IRow> RunEtl(IOperationProcess process)
        {
            process.InputProcess = new CreateRowsProcess(process.Context, "CreateRows")
            {
                Columns = SampleColumns,
                InputRows = SampleRows.ToList(),
            };

            var result = process.Evaluate().ToList();
            return result;
        }
    }
}