namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class TrimStringMutatorTests
{
    [TestMethod]
    public void SpecificColumns()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(new RowCreator()
        {
            Columns = ["Name", "Pets"],
            InputRows =
            [
                ["John, Oliver   ", "  Ubul"],
                [" Andrew, Smith", "Winston,Marley"],
            ],
        })
        .TrimString(new TrimStringMutator()
        {
            Columns = ["Name"],
        })
        .TrimString("Name");

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
                new() { ["Name"] = "John, Oliver", ["Pets"] = "  Ubul" },
                new() { ["Name"] = "Andrew, Smith", ["Pets"] = "Winston,Marley" } ]);

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}