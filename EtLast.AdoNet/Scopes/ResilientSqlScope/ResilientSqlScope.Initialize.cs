namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private bool Initialize(int initialExceptionCount)
    {
        if (Initializers == null)
            return true;

        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", round);
            try
            {
                using (var scope = Context.BeginScope(this, InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecuteInitializers();

                    if (Context.ExceptionCount == initialExceptionCount)
                        scope.Complete();
                } // dispose scope
            }
            catch (Exception ex)
            {
                AddException(ex);
            }

            if (Context.ExceptionCount == initialExceptionCount)
                return true;

            if (round < FinalizerRetryCount)
            {
                Context.ResetInternalCancellationToken();
                Context.ResetExceptionCount(initialExceptionCount);
            }
        }

        return false;
    }

    private void CreateAndExecuteInitializers()
    {
        IJob[] initializers;

        using (var creatorScope = Context.BeginScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
        {
            var builder = new ResilientSqlScopeProcessBuilder() { Scope = this };
            Initializers.Invoke(builder);
            initializers = builder.Jobs.Where(x => x != null).ToArray();

            Context.Log(LogSeverity.Information, this, "created {InitializerCount} initializers", initializers?.Length ?? 0);
        }

        if (initializers?.Length > 0)
        {
            Context.Log(LogSeverity.Information, this, "starting initializers");

            foreach (var process in initializers)
            {
                var preExceptionCount = Context.ExceptionCount;

                process.Execute(this);

                if (Context.ExceptionCount > preExceptionCount)
                    break;
            }
        }
    }
}