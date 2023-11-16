namespace FizzCode.EtLast.Tests.Unit.Mutators.Cross;

[TestClass]
public class BatchedKeyTestMutatorTests
{
    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<BatchedKeyTestMutator>();
    }

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void Complex(bool matchActionContainsMatch)
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .KeyTestBatched(new BatchedKeyTestMutator(context)
            {
                LookupBuilder = new FilteredRowLookupBuilder()
                {
                    ProcessCreator = _ => TestData.Country(context),
                    KeyGenerator = row => row.GenerateKey("id"),
                },
                RowKeyGenerator = row => row.GenerateKey("countryId"),
                NoMatchAction = new NoMatchAction(MatchMode.Custom)
                {
                    CustomAction = row =>
                    {
                        row["countryAbbrev"] = !row.HasValue("countryId")
                            ? "country was null"
                            : "no match found";
                    }
                },
                MatchAction = new MatchAction(MatchMode.Custom)
                {
                    CustomAction = (_, match) =>
                    {
                        if (matchActionContainsMatch)
                            Assert.IsNotNull(match);
                        else
                            Assert.IsNull(match);
                    },
                },
                MatchActionContainsMatch = matchActionContainsMatch,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["countryAbbrev"] = "country was null" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["countryAbbrev"] = "country was null" },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null, ["countryAbbrev"] = "no match found" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
