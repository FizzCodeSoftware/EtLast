namespace FizzCode.EtLast.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    public static class TestData
    {
        public static string[] CountryColumns { get; } = { "id", "name", "abbreviation2", "abbreviation3" };
        public static string[] PersonColumns { get; } = { "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime" };
        public static string[] PersonEyeColorColumns { get; } = { "id", "personId", "color" };
        public static string[] RoleHierarchyColumns { get; } = { "id", "code", "level1", "level2", "level3" };
        public static string[] PersonalAssetsPivotColumns { get; } = { "id", "personName", "cars", "houses", "kids" };

        [DataContract]
        public class PersonModel
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

        public static IProducer Country(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(Country))
            {
                Columns = CountryColumns,
                InputRows = new List<object[]>()
                {
                    new object[] { 1, "Hungary", "HU", "HUN" },
                    new object[] { 2, "United States of America", "US", "USA" },
                    new object[] { 3, "Spain", "ES", "ESP", },
                    new object[] { 4, "Mexico", "MX", "MEX" },
                }
            };
        }

        public static IProducer Person(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(Person))
            {
                Columns = PersonColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                    new object[] { 0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    new object[] { 3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            };
        }

        public static IProducer PersonSortedByName(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(Person))
            {
                Columns = PersonColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                    new object[] { 0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    new object[] { 3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            };
        }

        public static IProducer PersonChanged(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(Person))
            {
                Columns = PersonColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                    new object[] { 0, "A", 17, 160, "brown", 1, /*new DateTime(2010, 12, 9)*/ new DateTime(2010, 2, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    //new object[] { 3, "D", 39, 160, "fake", 3, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, /*160*/ 120, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 5, "A", /*11*/ null, 140, null, 3, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 6, "fake", /*null*/13, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            };
        }

        public static IProducer PersonEyeColor(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(PersonEyeColor))
            {
                Columns = PersonEyeColorColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "personId", "color"
                    new object[] { 0, 0, "yellow" },
                    new object[] { 1, 0, "red" },
                    new object[] { 2, 0, "green" },
                    new object[] { 3, 1, "blue" },
                    new object[] { 4, 1, "yellow" },
                    new object[] { 5, 2, "black" },
                    new object[] { 6, 100, "fake" },
                },
            };
        }

        public static IProducer RoleHierarchy(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(RoleHierarchy))
            {
                Columns = RoleHierarchyColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "code", "level1", "level2", "level3"
                    new object[] { 0, "A", "AAA" },
                    new object[] { 1, "B", null, "BBB" },
                    new object[] { 2, "C", null, null, "CCC" },
                    new object[] { 3, "D", null, null, "DDD" },
                    new object[] { 4, "E", null, "EEE" },
                    new object[] { 5, "F", null, "FFF" },
                },
            };
        }

        public static IProducer PersonalAssetsPivot(IEtlContext context)
        {
            return new RowCreator(context, null, nameof(PersonalAssetsPivot))
            {
                Columns = PersonalAssetsPivotColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "personName", "cars", "houses", "kids"
                    new object[] { 1, "A", 1, 1, 2 },
                    new object[] { null, "C", 2, 1, 3 },
                    new object[] { 3, "D", null, 1, 3 },
                    new object[] { 4, "E", "6", 1, 3 },
                },
            };
        }
    }
}