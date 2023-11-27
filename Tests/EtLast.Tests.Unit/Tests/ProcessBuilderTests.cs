namespace FizzCode.EtLast.Tests.Unit;

[TestClass]
public class ProcessBuilderTests
{
    [TestMethod]
    public void InputAndOneMutator()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .CustomCode(new CustomMutator()
        {
            Action = row => true,
        });

        var process = builder.Build();
        Assert.IsNotNull(process);
        Assert.IsTrue(process is CustomMutator);
        Assert.IsNotNull((process as CustomMutator).Input);
    }

    [TestMethod]
    public void InputAndTwoMutators()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person())
        .CustomCode(new CustomMutator()
        {
            Action = row => true,
        })
        .CustomCode(new CustomMutator()
        {
            Action = row => true,
        });

        var process = builder.Build();
        Assert.IsNotNull(process);
        Assert.IsTrue(process is CustomMutator);
        Assert.IsTrue((process as CustomMutator).Input is CustomMutator);
        Assert.IsNotNull(((process as CustomMutator).Input as CustomMutator).Input);
    }

    [TestMethod]
    public void InputOnly()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person());

        var process = builder.Build();
        Assert.IsNotNull(process);
        Assert.IsTrue(process is IRowSource);
    }
}