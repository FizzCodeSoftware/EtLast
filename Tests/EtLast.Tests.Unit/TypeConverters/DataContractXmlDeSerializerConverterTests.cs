namespace FizzCode.EtLast.Tests.Unit.TypeConverters
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Runtime.Serialization;
    using FizzCode.LightWeight.Collections;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DataContractXmlDeSerializerConverterTests
    {
        [TestMethod]
        public void ComplexTestCombinedWithSerializer()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(context),
                Mutators = new MutatorList()
                {
                    new InPlaceConvertMutator(context)
                    {
                        Columns = new[] { "birthDate" },
                        TypeConverter = new DateConverterAuto(new CultureInfo("hu-HU")),
                        ActionIfInvalid = InvalidValueAction.Throw,
                    },
                    new ExplodeMutator(context)
                    {
                        RowCreator = row =>
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
                        },
                    },
                    new DataContractXmlSerializerMutator<PersonModel>(context)
                    {
                         ColumnConfiguration = new ColumnCopyConfiguration("personModel", "personModelXml"),
                    },
                    new RemoveColumnMutator(context)
                    {
                        Columns = new[] { "personModel" },
                    },
                    new InPlaceConvertMutator(context)
                    {
                        Columns = new[] { "personModelXml" },
                        TypeConverter = new DataContractXmlDeSerializerConverter<PersonModel>(),
                    },
                    new RenameColumnMutator(context)
                    {
                        Columns = new()
                        {
                            ["personModelXml"] = "personModel",
                        },
                    },
                    new ExplodeMutator(context)
                    {
                        RowCreator = row =>
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
                        },
                    },
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(7, result.MutatedRows.Count);
            Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["birthDate"] = new DateTime(2018, 7, 11, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3},
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
            var exceptions = context.GetExceptions();
            Assert.AreEqual(0, exceptions.Count);
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
}