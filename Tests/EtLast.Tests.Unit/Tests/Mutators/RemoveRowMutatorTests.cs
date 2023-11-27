namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class RemoveRowMutatorTests
{
    [TestMethod]
    public void DelegateThrowsExceptionIf()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .RemoveRow(new RemoveRowMutator()
        {
            RowFilter = row => row.GetAs<int>("id") >= 4 && (row.GetAs<double>("id") == 7.0d),
        });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(4, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) } ]);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is ProcessExecutionException);
    }

    [TestMethod]
    public void RemoveAll()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .RemoveRow(new RemoveRowMutator()
        {
            RowFilter = _ => true,
        });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void RemoveNone()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .RemoveRow(new RemoveRowMutator()
        {
            RowFilter = _ => false,
        });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void RemoveSome()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .RemoveRow(new RemoveRowMutator()
        {
            RowFilter = row => row.GetAs<string>("name") == "A",
        });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(5, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}