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

        public static IOperationHostProcess CreateProcess()
        {
            var context = new EtlContext();

            return new OperationHostProcess(context)
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    MainLoopDelay = 10,
                    InputBufferSize = 10,
                }
            };
        }

        public List<IRow> RunEtl(IOperationHostProcess process)
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