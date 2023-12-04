namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DataContractXmlSerializerMutatorTests
{
    [TestMethod]
    public void CombinedTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .ConvertInPlace("birthDate").ToDateAuto(new CultureInfo("hu-HU")).KeepNull().ThrowIfInvalid()
            .Explode(new ExplodeMutator()
            {
                RemoveOriginalRow = true,
                RowCreator = row =>
                {
                    var newRow = new SlimRow
                    {
                        ["personModel"] = new TestData.PersonModel()
                        {
                            Id = row.GetAs<int>("id"),
                            Name = row.GetAs<string>("name"),
                            Age = row.GetAs<int?>("age"),
                            BirthDate = row.GetAs<DateTime?>("birthDate"),
                        }
                    };

                    return new[] { newRow };
                },
            })
            .Convert("personModel").Into("personModelXml").SerializeToDataContract().KeepNull().KeepInvalid()
            .RemoveColumn("personModel")
            .Convert("personModelXml").Into("personModel").DeserializeDataContractTo<TestData.PersonModel>().KeepNull().KeepInvalid()
            .Explode(new ExplodeMutator()
            {
                RemoveOriginalRow = true,
                RowCreator = row =>
                {
                    var personModel = row.GetAs<TestData.PersonModel>("personModel");
                    var newRow = new SlimRow()
                    {
                        ["id"] = personModel.Id,
                        ["name"] = personModel.Name,
                        ["age"] = personModel.Age,
                        ["birthDate"] = personModel.BirthDate,
                    };

                    return new[] { newRow };
                },
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0) },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0) },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["birthDate"] = new DateTime(2018, 7, 11, 0, 0, 0, 0) },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["birthDate"] = null },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
