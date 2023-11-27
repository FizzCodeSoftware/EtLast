namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class MergeStringColumnsMutatorTests
{
    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<MergeStringColumnsMutator>();
    }

    [TestMethod]
    public void Merge()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .MergeStringColumns(new MergeStringColumnsMutator()
            {
                ColumnsToMerge = ["name", "eyeColor"],
                Separator = "",
                TargetColumn = "Merged",
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = null, ["age"] = 17, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["Merged"] = "Abrown" },
            new() { ["id"] = 1, ["name"] = null, ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["Merged"] = "B" },
            new() { ["id"] = 2, ["name"] = null, ["age"] = 27, ["height"] = 170, ["eyeColor"] = null, ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["Merged"] = "Cgreen" },
            new() { ["id"] = 3, ["name"] = null, ["age"] = 39, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["Merged"] = "Dfake" },
            new() { ["id"] = 4, ["name"] = null, ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["Merged"] = "E" },
            new() { ["id"] = 5, ["name"] = null, ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["Merged"] = "A" },
            new() { ["id"] = 6, ["name"] = null, ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null, ["Merged"] = "fake" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
