namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class UnpivotMutatorTests
{
    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<UnpivotMutator>();
    }

    [TestMethod]
    public void FixColumnsIgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.PersonalAssetsPivot(context))
        .Unpivot(new UnpivotMutator(context)
        {
            FixColumns = new()
            {
                ["assetId"] = "id",
                ["personName"] = null
            },
            NewColumnForDimension = "asset-kind",
            NewColumnForValue = "amount",
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(12, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "cars", ["amount"] = null },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BothColumnsIgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.PersonalAssetsPivot(context))
        .Unpivot(new UnpivotMutator(context)
        {
            FixColumns = new()
            {
                ["assetId"] = "id",
                ["personName"] = null
            },
            NewColumnForDimension = "asset-kind",
            NewColumnForValue = "amount",
            ValueColumns = ["cars", "houses", "kids"],
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(11, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void BothColumnsKeepNull()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.PersonalAssetsPivot(context))
        .Unpivot(new UnpivotMutator(context)
        {
            FixColumns = new()
            {
                ["assetId"] = "id",
                ["personName"] = null,
            },
            NewColumnForDimension = "asset-kind",
            NewColumnForValue = "amount",
            IgnoreIfValueIsNull = false,
            ValueColumns = ["cars", "houses", "kids"],
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(12, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = null, ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "cars", ["amount"] = null },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["assetId"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void ValueColumnsIgnoreNull()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.PersonalAssetsPivot(context))
        .Unpivot(new UnpivotMutator(context)
        {
            ValueColumns = ["cars", "houses", "kids"],
            NewColumnForDimension = "asset-kind",
            NewColumnForValue = "amount",
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(11, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void ValueColumnsKeepNull()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.PersonalAssetsPivot(context))
        .Unpivot(new UnpivotMutator(context)
        {
            NewColumnForDimension = "asset-kind",
            NewColumnForValue = "amount",
            IgnoreIfValueIsNull = false,
            ValueColumns = ["cars", "houses", "kids"],
        });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(12, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "cars", ["amount"] = 1 },
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 1, ["personName"] = "A", ["asset-kind"] = "kids", ["amount"] = 2 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "cars", ["amount"] = 2 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = null, ["personName"] = "C", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "cars", ["amount"] = null },
            new() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 3, ["personName"] = "D", ["asset-kind"] = "kids", ["amount"] = 3 },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "cars", ["amount"] = "6" },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "houses", ["amount"] = 1 },
            new() { ["id"] = 4, ["personName"] = "E", ["asset-kind"] = "kids", ["amount"] = 3 } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}