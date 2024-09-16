namespace FizzCode.EtLast.Tests.Unit.Delimited;

[TestClass]
public class StructuredBinaryTableTests
{
    [TestMethod]
    public void WriteThenReadBackTest_Person()
    {
        var memoryStream = new MemoryStream();

        var context = TestExecuter.GetContext();

        var rows = TestData.Person().TakeRowsAndReleaseOwnership(null);

        var rowCache = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .BuildToInMemoryRowCache();

        var builder = SequenceBuilder.Fluent
            .ReadFromInMemoryRowCache(rowCache)
            .WriteToStructuredBinaryTable(new WriteToStructuredBinaryTableMutator()
            {
                DynamicColumns = () => new()
                {
                    ["id"] = typeof(int),
                    ["name"] = typeof(string),
                    ["age"] = typeof(int),
                    ["height"] = typeof(int),
                    ["eyeColor"] = typeof(string),
                    ["countryId"] = typeof(int),
                    ["birthDate"] = typeof(DateTime),
                    ["lastChangedTime"] = typeof(DateTime),
                },
                SinkProvider = new MemorySinkProvider()
                {
                    Stream = memoryStream,
                    AutomaticallyDispose = false,
                },
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(rowCache.CurrentRowCount, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, rowCache.TakeRowsAndReleaseOwnership(context)
            .Select(x => x.Values.ToDictionary()).ToList());
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);

        memoryStream.Position = 0;

        context = TestExecuter.GetContext();
        builder = SequenceBuilder.Fluent
            .ReadStructuredBinaryTable(new StructuredBinaryTableReader()
            {
                StreamProvider = new OneMemoryStreamProvider()
                {
                    Stream = memoryStream,
                },
            });

        result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(rowCache.CurrentRowCount, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, rowCache.TakeRowsAndReleaseOwnership(context)
            .Select(x => x.Values.ToDictionary()).ToList());
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void WriteThenReadBackTest_RandomValuesTest()
    {
        var memoryStream = new MemoryStream();

        var context = TestExecuter.GetContext();

        var rows = TestData.Person().TakeRowsAndReleaseOwnership(null);

        var rowCache = SequenceBuilder.Fluent
            .ImportEnumerable(new EnumerableImporter()
            {
                InputGenerator = _ => new[]
                {
                    new SlimRow() { ["id"] = 01, ["value"] = "dog", },
                    new SlimRow() { ["id"] = 02, ["value"] = 6, },
                    new SlimRow() { ["id"] = 03, ["value"] = 9L, },
                    new SlimRow() { ["id"] = 04, ["value"] = (byte)1, },
                    new SlimRow() { ["id"] = 05, ["value"] = 1789.1212121221d, },
                    new SlimRow() { ["id"] = 06, ["value"] = 98.000000000001f, },
                    new SlimRow() { ["id"] = 07, ["value"] = new DateOnly(2001, 1, 1), },
                    new SlimRow() { ["id"] = 08, ["value"] = new TimeOnly(12, 0, 59, 59, 999) },
                    new SlimRow() { ["id"] = 09, ["value"] = Guid.NewGuid(), },
                    new SlimRow() { ["id"] = 10, ["value"] = 67676767676767676767.6767676767m },
                    new SlimRow() { ["id"] = 11, ["value"] = uint.MaxValue, },
                    new SlimRow() { ["id"] = 12, ["value"] = ulong.MaxValue, },
                    new SlimRow() { ["id"] = "13a", ["value"] = new Int128(512ul, 128ul), },
                    new SlimRow() { ["id"] = "13b", ["value"] = new UInt128(2ul, ulong.MaxValue), },
                    new SlimRow() { ["id"] = 14, ["value"] = new byte[] { 1,2,3,4,5 }, },
                    new SlimRow() { ["id"] = 15, ["value"] = 'c', },
                    new SlimRow() { ["id"] = 16, ["value"] = null, },
                    new SlimRow() { ["id"] = 17, ["value"] = (short)-1, },
                    new SlimRow() { ["id"] = 18, ["value"] = (ushort)128, },
                    new SlimRow() { ["id"] = 19, ["value"] = new DateTimeOffset(2001, 12, 30, 23, 59, 59, 999, TimeSpan.FromHours(2)), },
                    new SlimRow() { ["id"] = 20, ["value"] = TimeSpan.FromMilliseconds(8192.5d), },
                }
            })
            .BuildToInMemoryRowCache();

        var builder = SequenceBuilder.Fluent
            .ReadFromInMemoryRowCache(rowCache)
            .WriteToStructuredBinaryTable(new WriteToStructuredBinaryTableMutator()
            {
                DynamicColumns = () => new()
                {
                    ["id"] = typeof(int),
                    ["value"] = typeof(int),
                },
                SinkProvider = new MemorySinkProvider()
                {
                    Stream = memoryStream,
                    AutomaticallyDispose = false,
                },
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(rowCache.CurrentRowCount, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, rowCache.TakeRowsAndReleaseOwnership(context)
            .Select(x => x.Values.ToDictionary()).ToList());
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);

        memoryStream.Position = 0;

        context = TestExecuter.GetContext();
        builder = SequenceBuilder.Fluent
            .ReadStructuredBinaryTable(new StructuredBinaryTableReader()
            {
                StreamProvider = new OneMemoryStreamProvider()
                {
                    Stream = memoryStream,
                },
            });

        result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(rowCache.CurrentRowCount, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, rowCache.TakeRowsAndReleaseOwnership(context)
            .Select(x => x.Values.ToDictionary()).ToList());
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}