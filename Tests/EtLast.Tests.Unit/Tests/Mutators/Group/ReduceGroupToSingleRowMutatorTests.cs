namespace FizzCode.EtLast.Tests.Unit.Mutators.Aggregation;

[TestClass]
public class ReduceGroupToSingleRowMutatorTests
{
    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<ReduceGroupToSingleRowMutator>();
    }

    [TestMethod]
    public void PartialAndFullRemoval()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .ConvertValue(new InPlaceConvertMutator(context)
            {
                Columns = new[] { "age" },
                TypeConverter = new DecimalConverter(),
            })
            .ReduceGroupToSingleRow(new ReduceGroupToSingleRowMutator(context)
            {
                KeyGenerator = row => row.GenerateKey("name"),
                Selector = (proc, groupRows) =>
                {
                    return groupRows
                        .Where(x => x.HasValue("age"))
                        .OrderBy(x => x.GetAs<decimal>("age"))
                        .FirstOrDefault();
                }
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(5, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11m, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8m, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27m, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39m, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3m, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IgnoreSelectorForSingleRowGroupsTrue()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .ConvertValue(new InPlaceConvertMutator(context)
            {
                Columns = new[] { "age" },
                TypeConverter = new DecimalConverter(),
            })
            .ReduceGroupToSingleRow(new ReduceGroupToSingleRowMutator(context)
            {
                IgnoreSelectorForSingleRowGroups = true,
                KeyGenerator = row => row.GenerateKey("name"),
                Selector = (proc, groupRows) =>
                {
                    return groupRows
                        .Where(x => x.HasValue("age"))
                        .OrderBy(x => x.GetAs<decimal>("age"))
                        .FirstOrDefault();
                },
            });

        // note: this includes "fake" because Selector is not applied over single-row groups

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(6, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11m, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8m, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27m, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39m, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3m, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void IgnoreSelectorForSingleRowGroupsDefaultFalse()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .ConvertValue(new InPlaceConvertMutator(context)
            {
                Columns = new[] { "age" },
                TypeConverter = new DecimalConverter(),
            })
            .ReduceGroupToSingleRow(new ReduceGroupToSingleRowMutator(context)
            {
                KeyGenerator = row => row.GenerateKey("name"),
                Selector = (proc, groupRows) =>
                {
                    if (groupRows.Count < 2)
                        throw new EtlException("wrong");

                    return groupRows[0];
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(1, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17m, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) } });
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is EtlException);
    }
}
