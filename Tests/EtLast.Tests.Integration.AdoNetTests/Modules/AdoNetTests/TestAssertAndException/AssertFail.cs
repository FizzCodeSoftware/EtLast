﻿namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class AssertFail : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override IEnumerable<IProcess> CreateJobs(IProcess caller)
    {
        yield return new CustomJob(Context)
        {
            Name = "StoredProcedureAdoNetDbReader",
            Action = job =>
            {
                Assert.Fail("Expected fail from Assert TestAssertAndException");
            }
        };
    }
}
