namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractBaseTestUsingSeed
    {
        public string[] SeedColumnNames { get; } = { "id", "name", "age", "fkid", "date", "time", "datetime" };

        public IOperationProcess CreateProcess(IEtlContext context = null)
        {
            context ??= new EtlContext<DictionaryRow>();

            return new OperationProcess(context)
            {
                Configuration = new OperationProcessConfiguration()
                {
                    MainLoopDelay = 10,
                    InputBufferSize = 10,
                }
            };
        }

        public List<IRow> RunEtl(IOperationProcess process, int rowCount)
        {
            var inputProcess = new SeedRowsProcess(process.Context, "SeedRows")
            {
                Count = rowCount,
                Columns = SeedColumnNames,
            };

            process.InputProcess = inputProcess;

            var result = process.Evaluate().ToList();
            return result;
        }
    }
}