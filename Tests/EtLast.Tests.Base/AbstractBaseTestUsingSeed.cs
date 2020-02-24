namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractBaseTestUsingSeed
    {
        public string[] SeedColumnNames { get; } = { "id", "name", "age", "fkid", "date", "time", "datetime" };

        public ProcessBuilder CreateProcessBuilder(int rowCount, ITopic topic)
        {
            return new ProcessBuilder()
            {
                InputProcess = new SeedRowsProcess(topic, "SeedRows")
                {
                    Count = rowCount,
                    Columns = SeedColumnNames,
                },
                Mutators = new MutatorList(),
            };
        }

        public static List<IRow> RunBuilder(ProcessBuilder builder)
        {
            var result = builder.Build().Evaluate().TakeRowsAndReleaseOwnership().ToList();
            return result;
        }
    }
}