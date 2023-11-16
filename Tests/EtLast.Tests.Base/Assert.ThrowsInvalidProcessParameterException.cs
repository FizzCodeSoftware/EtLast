namespace FizzCode.EtLast.Tests;

public static class ThrowsInvalidProcessParameterExceptionHelper
{
    public static void ThrowsInvalidProcessParameterException<T>(this Assert assert)
        where T : IMutator
    {
        ArgumentNullException.ThrowIfNull(assert);

        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
        .ReadFrom(TestData.Person(context))
        .AddMutator((T)Activator.CreateInstance(typeof(T), context));

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(0, result.MutatedRows.Count);
        Assert.AreEqual(1, result.Process.FlowState.Exceptions.Count);
        Assert.IsTrue(result.Process.FlowState.Exceptions[0] is InvalidProcessParameterException);
    }
}