namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractBaseTestUsingSeed
    {
        public string[] SeedColumnNames { get; } = { "id", "name", "age", "fkid", "date", "time", "datetime" };

        public static IOperationHostProcess CreateProcess(IEtlContext context = null)
        {
            context ??= new EtlContext<DictionaryRow>();

            return new OperationHostProcess(context)
            {
                Configuration = new OperationHostProcessConfiguration()
                {
                    MainLoopDelay = 10,
                    InputBufferSize = 10,
                }
            };
        }

        public List<IRow> RunEtl(IOperationHostProcess process, int rowCount)
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