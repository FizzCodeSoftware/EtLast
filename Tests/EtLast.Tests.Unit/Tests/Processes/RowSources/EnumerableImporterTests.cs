namespace FizzCode.EtLast.Tests.Unit.Producers;

[TestClass]
public class EnumerableImporterTests
{
    [TestMethod]
    public void FullCopy()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ImportEnumerable(new EnumerableImporter(context)
        {
            InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void CopyOnlySpecifiedColumnsOff()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ImportEnumerable(new EnumerableImporter(context)
        {
            InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            Columns = new()
            {
                ["ID"] = new ReaderColumn(),
                ["age"] = new ReaderColumn().ValueWhenSourceIsNull(-1L),
            },
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["ID"] = 0, ["age"] = 17, ["name"] = "A", ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["ID"] = 1, ["age"] = 8, ["name"] = "B", ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["ID"] = 2, ["age"] = 27, ["name"] = "C", ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["ID"] = 3, ["age"] = 39, ["name"] = "D", ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["ID"] = 4, ["age"] = -3, ["name"] = "E", ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["ID"] = 5, ["age"] = 11, ["name"] = "A", ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["ID"] = 6, ["age"] = -1L, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void CopyOnlySpecifiedColumnsOn()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ImportEnumerable(new EnumerableImporter(context)
        {
            InputGenerator = caller => TestData.Person(context).TakeRowsAndReleaseOwnership(caller),
            Columns = new()
            {
                ["ID"] = new ReaderColumn(),
                ["age"] = new ReaderColumn().ValueWhenSourceIsNull(-1L),
            },
            CopyOnlySpecifiedColumns = true,
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["ID"] = 0, ["age"] = 17 },
            new() { ["ID"] = 1, ["age"] = 8 },
            new() { ["ID"] = 2, ["age"] = 27 },
            new() { ["ID"] = 3, ["age"] = 39 },
            new() { ["ID"] = 4, ["age"] = -3 },
            new() { ["ID"] = 5, ["age"] = 11 },
            new() { ["ID"] = 6, ["age"] = -1L } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}