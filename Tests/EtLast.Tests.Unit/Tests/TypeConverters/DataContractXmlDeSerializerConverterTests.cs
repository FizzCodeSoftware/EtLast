using System.Runtime.Serialization;

namespace FizzCode.EtLast.Tests.Unit.TypeConverters;

[TestClass]
public class DataContractXmlDeSerializerConverterTests
{
    [TestMethod]
    public void ComplexTestCombinedWithSerializer()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person(context))
        .ConvertValue(new InPlaceConvertMutator(context)
        {
            Columns = ["birthDate"],
            TypeConverter = new DateConverterAuto(new CultureInfo("hu-HU")),
            ActionIfInvalid = InvalidValueAction.Throw,
        })
        .Explode("ReplaceWithModel", row =>
        {
            var newRow = new SlimRow
            {
                ["personModel"] = new PersonModel()
                {
                    Id = row.GetAs<int>("id"),
                    Name = row.GetAs<string>("name"),
                    Age = row.GetAs<int?>("age"),
                    BirthDate = row.GetAs<DateTime?>("birthDate"),
                }
            };

            return new[] { newRow };
        })
        .SerializeToXml(new DataContractXmlSerializerMutator<PersonModel>(context)
        {
            SourceColumn = "personModel",
            TargetColumn = "personModelXml",
            ActionIfFailed = InvalidValueAction.Keep,
        })
        .RemoveColumn("personModel")
        .ConvertValue(new InPlaceConvertMutator(context)
        {
            Columns = ["personModelXml"],
            TypeConverter = new DataContractXmlDeSerializerConverter<PersonModel>(),
        })
        .RenameColumn("personModelXml", "personModel")
        .Explode("ReplaceWithColumns", row =>
        {
            var personModel = row.GetAs<PersonModel>("personModel");
            var newRow = new SlimRow()
            {
                ["id"] = personModel.Id,
                ["name"] = personModel.Name,
                ["age"] = personModel.Age,
                ["birthDate"] = personModel.BirthDate,
            };

            return new[] { newRow };
        });

        var result = TestExecuter.Execute(builder);
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

    [DataContract]
    private class PersonModel
    {
        [DataMember]
        public int Id { get; set; }

        [DataMember]
        public string Name { get; set; }

        [DataMember]
        public int? Age { get; set; }

        [DataMember]
        public DateTime? BirthDate { get; set; }
    }
}