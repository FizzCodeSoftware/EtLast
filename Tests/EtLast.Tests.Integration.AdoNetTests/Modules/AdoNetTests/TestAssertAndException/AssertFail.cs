namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class AssertFail : AbstractEtlTask
{
    public override void Execute(IFlow flow)
    {
        flow.CustomJob("StoredProcedureAdoNetDbReader", job =>
        {
            Assert.Fail("Expected fail from Assert TestAssertAndException");
        });
    }
}
