namespace FizzCode.EtLast.Tests.Base
{
    using System.Collections.Generic;
    using System.Linq;

    public abstract class AbstractBaseTestUsingSeed
    {
        public string[] SeedColumnNames { get; } = { "id", "name", "age", "fkid", "date", "time", "datetime" };

        public MutatorBuilder CreateMutatorBuilder(int rowCount, IEtlContext context)
        {
            return new MutatorBuilder()
            {
                InputProcess = new SeedRowsProcess(context, "SeedRows", null)
                {
                    Count = rowCount,
                    Columns = SeedColumnNames,
                },
                Mutators = new List<IMutator>(),
            };
        }

        public static List<IRow> RunEtl(MutatorBuilder builder)
        {
            var result = builder.BuildEvaluable().Evaluate().TakeRowsAndReleaseOwnership().ToList();
            return result;
        }
    }
}