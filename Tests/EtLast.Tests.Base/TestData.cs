using System.Runtime.Serialization;

namespace FizzCode.EtLast.Tests;

public static class TestData
{
    public static string[] CountryColumns { get; } = ["id", "name", "abbreviation2", "abbreviation3"];
    public static string[] PersonColumns { get; } = ["id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"];
    public static string[] PersonEyeColorColumns { get; } = ["id", "personId", "color"];
    public static string[] RoleHierarchyColumns { get; } = ["id", "code", "level1", "level2", "level3"];
    public static string[] PersonalAssetsPivotColumns { get; } = ["id", "personName", "cars", "houses", "kids"];

    public static List<object[]> CountryData =>
    [
        [1, "Hungary", "HU", "HUN"],
        [2, "United States of America", "US", "USA"],
        [3, "Spain", "ES", "ESP",],
        [4, "Mexico", "MX", "MEX"],
    ];

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

    public static ISequence Country()
    {
        return new RowCreator()
        {
            Columns = CountryColumns,
            InputRows = CountryData
        };
    }

    public static ISequence Person()
    {
        return new RowCreator()
        {
            Columns = PersonColumns,
            InputRows =
            [
                // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                [0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1)],
                [1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0)],
                [2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58)],
                [3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1)],
                [4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59)],
                [5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0)],
                [6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null],
            ],
        };
    }

    public static ISequence PersonSortedByName()
    {
        return new RowCreator()
        {
            Columns = PersonColumns,
            InputRows =
            [
                // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                [0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1)],
                [5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0)],
                [1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0)],
                [2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58)],
                [3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1)],
                [4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59)],
                [6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null],
            ],
        };
    }

    public static ISequence PersonChanged()
    {
        return new RowCreator()
        {
            Columns = PersonColumns,
            InputRows =
            [
                // "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime"
                [0, "A", 17, 160, "brown", 1, /*new DateTime(2010, 12, 9)*/ new DateTime(2010, 2, 9), new DateTime(2015, 12, 19, 12, 0, 1)],
                [1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0)],
                [2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58)],
                //new object[] { 3, "D", 39, 160, "fake", 3, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                [4, "E", -3, /*160*/ 120, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59)],
                [5, "A", /*11*/ null, 140, null, 3, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0)],
                [6, "fake", /*null*/13, 140, null, 5, new DateTime(2018, 1, 9), null],
            ],
        };
    }

    public static ISequence PersonEyeColor()
    {
        return new RowCreator()
        {
            Columns = PersonEyeColorColumns,
            InputRows =
            [
                // "id", "personId", "color"
                [0, 0, "yellow"],
                [1, 0, "red"],
                [2, 0, "green"],
                [3, 1, "blue"],
                [4, 1, "yellow"],
                [5, 2, "black"],
                [6, 100, "fake"],
            ],
        };
    }

    public static ISequence RoleHierarchy()
    {
        return new RowCreator()
        {
            Columns = RoleHierarchyColumns,
            InputRows =
            [
                // "id", "code", "level1", "level2", "level3"
                [0, "A", "AAA"],
                [1, "B", null, "BBB"],
                [2, "C", null, null, "CCC"],
                [3, "D", null, null, "DDD"],
                [4, "E", null, "EEE"],
                [5, "F", null, "FFF"],
            ],
        };
    }

    public static ISequence PersonalAssetsPivot()
    {
        return new RowCreator()
        {
            Columns = PersonalAssetsPivotColumns,
            InputRows =
            [
                // "id", "personName", "cars", "houses", "kids"
                [1, "A", 1, 1, 2],
                [null, "C", 2, 1, 3],
                [3, "D", null, 1, 3],
                [4, "E", "6", 1, 3],
            ],
        };
    }
}
