namespace FizzCode.EtLast.Tests
{
    using System;
    using System.Collections.Generic;

    public static class TestData
    {
        public static string[] CountryColumns { get; } = { "id", "name", "abbreviation2", "abbreviation3" };
        public static string[] PersonColumns { get; } = { "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime" };
        public static string[] PersonEyeColorColumns { get; } = { "id", "personId", "color" };
        public static string[] RoleHierarchyColumns { get; } = { "id", "name", "level1", "level2", "level3" };
        public static string[] PersonalAssetsPivotColumns { get; } = { "id", "personName", "cars", "houses", "kids" };

        public static IEvaluable Country(ITopic topic)
        {
            return new RowCreator(topic, nameof(Country))
            {
                Columns = CountryColumns,
                InputRows = new List<object[]>()
                {
                    new object[] { 1, "Hungary", "HU", "HUN" },
                    new object[] { 2, "United States of America", "US", "USA" },
                    new object[] { 3, "Spain", "ES", "ESP", },
                    new object[] { 4, "Mexico", "MX", "MEX" },
                    new object[] { 5, "Fake", "FK", null },
                }
            };
        }

        public static IEvaluable Person(ITopic topic)
        {
            return new RowCreator(topic, nameof(Person))
            {
                Columns = PersonColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                    new object[] { 0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    new object[] { 3, "D", 39, 160, "fake", 3, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 5, "A", 11, 140, null, 3, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            };
        }

        public static IEvaluable PersonEyeColor(ITopic topic)
        {
            return new RowCreator(topic, nameof(PersonEyeColor))
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

        public static IEvaluable RoleHierarchy(ITopic topic)
        {
            return new RowCreator(topic, nameof(RoleHierarchy))
            {
                Columns = RoleHierarchyColumns,
                InputRows = new List<object[]>()
                {
                    // "id", "name", "level1", "level2", "level3"
                    new object[] { 0, "A", "AAA" },
                    new object[] { 1, "B", null, "BBB" },
                    new object[] { 2, "C", null, null, "CCC" },
                    new object[] { 3, "D", null, null, "DDD" },
                    new object[] { 4, "E", null, "EEE" },
                    new object[] { 5, "F", null, "FFF" },
                },
            };
        }

        public static IEvaluable PersonalAssetsPivot(ITopic topic)
        {
            return new RowCreator(topic, nameof(PersonalAssetsPivot))
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