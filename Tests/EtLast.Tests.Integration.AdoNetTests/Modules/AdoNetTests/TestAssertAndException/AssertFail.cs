﻿namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class AssertFail : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow.ContinueWithProcess(() => new CustomJob(Context)
        {
            Name = "StoredProcedureAdoNetDbReader",
            Action = job => Assert.Fail("Expected fail from Assert TestAssertAndException"),
        });
    }
}
