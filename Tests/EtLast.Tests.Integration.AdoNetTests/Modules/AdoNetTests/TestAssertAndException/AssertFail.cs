namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class AssertFail : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override IEnumerable<IExecutable> CreateProcesses()
    {
        yield return new CustomAction(Context)
        {
            Name = "StoredProcedureAdoNetDbReader",
            Action = proc =>
            {
                Assert.Fail("Expected fail from Assert TestAssertAndException");
            }
        };
    }
}
