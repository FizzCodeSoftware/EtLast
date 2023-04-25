using System.Globalization;
using System.IO;

namespace FizzCode.EtLast.Tests.EPPlus;

[TestClass]
public class EpPlusSimpleRowWriterTests
{
    [TestMethod]
    public void OnePartitionWriteTest()
    {
        const string directory = @".\Temp-" + nameof(OnePartitionWriteTest);
        if (Directory.Exists(directory))
            Directory.Delete(directory, true);

        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .UsePredefinedRows(new RowCreator(context)
            {
                Columns = new[] { "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime" },
                InputRows = new List<object[]>()
                {
                    new object[] { 0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    new object[] { 3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            })
            .WriteRowToExcelSimple(new EpPlusSimpleRowWriterMutator(context)
            {
                Columns = new()
                {
                    ["id"] = null,
                    ["name"] = null,
                    ["age"] = null,
                    ["height"] = null,
                    ["eyeColor"] = null,
                    ["countryId"] = null,
                    ["birth date"] = new ExcelColumn().FromSource("birthDate").SetNumberFormat("yyyy-mm-dd"),
                    ["lastChangedTime"] = new ExcelColumn().SetNumberFormat("yyyy-mm-dd hh:mm"),
                },
                PartitionKeyGenerator = null,
                SinkProvider = new LocalFileSinkProvider()
                {
                    FileNameGenerator = partition => directory + "\\test.xlsx",
                    ActionWhenFileExists = LocalSinkFileExistsAction.DeleteAndContinue,
                    FileMode = FileMode.OpenOrCreate,
                    FileAccess = FileAccess.ReadWrite,
                },
                SheetName = "person",
                Finalize = (package, state) =>
                {
                    state.Worksheet.Cells.AutoFitColumns();
                    state.Worksheet.View.FreezePanes(2, 1);
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);

        builder = SequenceBuilder.Fluent
            .ReadFromExcel(new EpPlusExcelReader(context)
            {
                SheetName = "person",
                StreamProvider = new LocalDirectoryStreamProvider()
                {
                    Directory = directory,
                    SearchPattern = "*.xlsx",
                },
                Columns = new()
                {
                    ["id"] = new ReaderColumn(new IntConverter()),
                    ["name"] = new ReaderColumn(),
                    ["age"] = new ReaderColumn(new IntConverter()),
                    ["height"] = new ReaderColumn(new IntConverter()),
                    ["eyeColor"] = new ReaderColumn(),
                    ["countryId"] = new ReaderColumn(new IntConverter()),
                    ["birth date"] = new ReaderColumn(new StringConverter()),
                    ["lastChangedTime"] = new ReaderColumn(new DoubleConverter()),
                },
            });

        result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birth date"] = "40521", ["lastChangedTime"] = 42357.50001157408d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birth date"] = "40575", ["lastChangedTime"] = 42357.54305555556d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birth date"] = "41660", ["lastChangedTime"] = 42329.71664351852d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birth date"] = "2018.07.11", ["lastChangedTime"] = 42948.17292824074d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = 43466.99998842592d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birth date"] = "41409", ["lastChangedTime"] = 43101d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birth date"] = "43109" } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void ManyPartitionWriteTest()
    {
        const string directory = @".\Temp-" + nameof(ManyPartitionWriteTest);
        if (Directory.Exists(directory))
            Directory.Delete(directory, true);

        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .UsePredefinedRows(new RowCreator(context)
            {
                Columns = new[] { "id", "name", "age", "height", "eyeColor", "countryId", "birthDate", "lastChangedTime" },
                InputRows = new List<object[]>()
                {
                    new object[] { 0, "A", 17, 160, "brown", 1, new DateTime(2010, 12, 9), new DateTime(2015, 12, 19, 12, 0, 1) },
                    new object[] { 1, "B", 8, 190, null, 1, new DateTime(2011, 2, 1), new DateTime(2015, 12, 19, 13, 2, 0) },
                    new object[] { 2, "C", 27, 170, "green", 2, new DateTime(2014, 1, 21), new DateTime(2015, 11, 21, 17, 11, 58) },
                    new object[] { 3, "D", 39, 160, "fake", null, "2018.07.11", new DateTime(2017, 8, 1, 4, 9, 1) },
                    new object[] { 4, "E", -3, 160, null, 1, null, new DateTime(2019, 1, 1, 23, 59, 59) },
                    new object[] { 5, "A", 11, 140, null, null, new DateTime(2013, 5, 15), new DateTime(2018, 1, 1, 0, 0, 0) },
                    new object[] { 6, "fake", null, 140, null, 5, new DateTime(2018, 1, 9), null },
                },
            })
            .WriteRowToExcelSimple(new EpPlusSimpleRowWriterMutator(context)
            {
                Columns = new()
                {
                    ["id"] = null,
                    ["name"] = null,
                    ["age"] = null,
                    ["height"] = null,
                    ["eyeColor"] = null,
                    ["countryId"] = null,
                    ["birth date"] = new ExcelColumn().FromSource("birthDate").SetNumberFormat("yyyy-mm-dd"),
                    ["lastChangedTime"] = new ExcelColumn().SetNumberFormat("yyyy-mm-dd hh:mm"),
                },
                PartitionKeyGenerator = (row, index) => (index % 2).ToString("D", CultureInfo.InvariantCulture),
                SinkProvider = new LocalFileSinkProvider()
                {
                    FileNameGenerator = partition => directory + "\\test-" + partition + ".xlsx",
                    ActionWhenFileExists = LocalSinkFileExistsAction.DeleteAndContinue,
                    FileMode = FileMode.OpenOrCreate,
                    FileAccess = FileAccess.ReadWrite,
                },
                SheetName = "person",
                Finalize = (package, state) =>
                {
                    state.Worksheet.Cells.AutoFitColumns();
                    state.Worksheet.View.FreezePanes(2, 1);
                },
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
                new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);

        builder = SequenceBuilder.Fluent
            .ReadFromExcel(new EpPlusExcelReader(context)
            {
                SheetName = "person",
                StreamProvider = new LocalDirectoryStreamProvider()
                {
                    Directory = directory,
                    SearchPattern = "*.xlsx",
                },
                Columns = new()
                {
                    ["id"] = new ReaderColumn(new IntConverter()),
                    ["name"] = new ReaderColumn(),
                    ["age"] = new ReaderColumn(new IntConverter()),
                    ["height"] = new ReaderColumn(new IntConverter()),
                    ["eyeColor"] = new ReaderColumn(),
                    ["countryId"] = new ReaderColumn(new IntConverter()),
                    ["birth date"] = new ReaderColumn(new StringConverter()),
                    ["lastChangedTime"] = new ReaderColumn(new DoubleConverter()),
                },
            });

        result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birth date"] = "40521", ["lastChangedTime"] = 42357.50001157408d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birth date"] = "41660", ["lastChangedTime"] = 42329.71664351852d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = 43466.99998842592d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birth date"] = "43109" },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birth date"] = "40575", ["lastChangedTime"] = 42357.54305555556d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birth date"] = "2018.07.11", ["lastChangedTime"] = 42948.17292824074d },
            new CaseInsensitiveStringKeyDictionary<object>() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birth date"] = "41409", ["lastChangedTime"] = 43101d } });
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}