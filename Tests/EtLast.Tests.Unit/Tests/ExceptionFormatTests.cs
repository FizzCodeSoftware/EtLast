namespace FizzCode.EtLast.Tests.Unit.Exceptions;

[TestClass]
public class ExceptionFormatTests
{
    [TestMethod]
    public void DummyForDevelopment1()
    {
        var context = TestExecuter.GetContext();

        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .CustomCode(new CustomMutator(context)
            {
                Action = _ => throw new Exception("ohh"),
            });

        var process = builder.Build();
        process.Execute(null);
        var msg = process.FlowState.Exceptions[0].FormatExceptionWithDetails(true);
        Debug.WriteLine(msg);
        //Debugger.Break();
    }

    [TestMethod]
    public void DummyForDevelopment2()
    {
        var context = TestExecuter.GetContext();

        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .CustomCode(new CustomMutator(context)
            {
                Action = null,
            });

        var process = builder.Build();
        process.Execute(null);
        var msg = process.FlowState.Exceptions[0].FormatExceptionWithDetails(true);
        Debug.WriteLine(msg);
        //Debugger.Break();
    }

    [TestMethod]
    public void DummyForDevelopment3()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .Join(new JoinMutator(context)
            {
                LookupBuilder = new RowLookupBuilder()
                {
                    Process = TestData.PersonEyeColor(context),
                    KeyGenerator = row => row.GenerateKey("personId"),
                },
                RowKeyGenerator = row => row.GenerateKey("id"),
                NoMatchAction = new NoMatchAction(MatchMode.Throw),
                Columns = new(),
            });

        var process = builder.Build();
        process.Execute(null);
        var msg = process.FlowState.Exceptions[0].FormatExceptionWithDetails(true);
        Debug.WriteLine(msg);
        //Debugger.Break();
    }

    [TestMethod]
    public void DummyForDevelopment4()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .Join(new JoinMutator(context)
            {
                LookupBuilder = new RowLookupBuilder()
                {
                    Process = TestData.PersonEyeColor(context),
                    KeyGenerator = row => row.GenerateKey("personId"),
                },
                RowKeyGenerator = row => row.GenerateKey("id"),
                NoMatchAction = new NoMatchAction(MatchMode.Custom)
                {
                    CustomAction = _ => throw new Exception("ohh"),
                },
                Columns = new(),
            });

        var process = builder.Build();
        process.Execute(null);
        var msg = process.FlowState.Exceptions[0].FormatExceptionWithDetails(true);
        Debug.WriteLine(msg);
        //Debugger.Break();
    }

    [TestMethod]
    public void DummyForDevelopment5()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .Join(new JoinMutator(context)
            {
                LookupBuilder = new RowLookupBuilder()
                {
                    Process = TestData.PersonEyeColor(context),
                    KeyGenerator = row => row.GenerateKey("personId"),
                },
                RowKeyGenerator = row => row.GenerateKey("id"),
                MatchCustomAction = (_, _) => throw new Exception("ohh"),
                Columns = new(),
            });

        var process = builder.Build();
        process.Execute(null);
        var msg = process.FlowState.Exceptions[0].FormatExceptionWithDetails(true);
        Debug.WriteLine(msg);
        //Debugger.Break();
    }
}
