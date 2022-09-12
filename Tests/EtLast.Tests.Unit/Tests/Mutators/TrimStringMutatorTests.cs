namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class TrimStringMutatorTests
{
    [TestMethod]
    public void SpecificColumns()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(new RowCreator(context)
        {
            Columns = new[] { "Name", "Pets" },
            InputRows = new List<object[]>()
            {
                new object[] { "John, Oliver   ", "  Ubul" },
                new object[] { " Andrew, Smith", "Winston,Marley" },
            },
        })
        .TrimString(new TrimStringMutator(context)
        {
            Columns = new[] { "Name" },
        })
        .TrimString("Name");

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(2, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "John, Oliver", ["Pets"] = "  Ubul" },
                new CaseInsensitiveStringKeyDictionary<object>() { ["Name"] = "Andrew, Smith", ["Pets"] = "Winston,Marley" } });

        Assert.AreEqual(0, result.Process.InvocationContext.Exceptions.Count);
    }
}