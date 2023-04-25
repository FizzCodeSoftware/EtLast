namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DataContractXmlSerializerMutatorTests
{
    [TestMethod]
    public void CombinedTest()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .ConvertValue(new InPlaceConvertMutator(context)
            {
                Columns = new[] { "birthDate" },
                TypeConverter = new DateConverterAuto(new CultureInfo("hu-HU"))
                {
                    EpochDate = null,
                },
                ActionIfInvalid = InvalidValueAction.Throw,
            })
            .Explode(new ExplodeMutator(context)
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
            .SerializeToXml(new DataContractXmlSerializerMutator<TestData.PersonModel>(context)
            {
                SourceColumn = "personModel",
                TargetColumn = "personModelXml",
                ActionIfFailed = InvalidValueAction.Keep,
            })
            .RemoveColumn(new RemoveColumnMutator(context)
            {
                Columns = new[] { "personModel" },
            })
            .DeSerializeFromXml(new DataContractXmlDeSerializerMutator<TestData.PersonModel>(context)
            {
                SourceColumn = "personModelXml",
                TargetColumn = "personModel",
                ActionIfFailed = InvalidValueAction.Keep,
            })
            .Explode(new ExplodeMutator(context)
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

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["birthDate"] = new DateTime(2018, 7, 11, 0, 0, 0, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3 },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0) },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}
