namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private void Initialize(ProcessInvocationContext invocationContext)
    {
        if (Initializers == null)
            return;

        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (InvocationContext.IsTerminating)
                return;

            Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", round);
            try
            {
                using (var scope = Context.BeginScope(this, InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecuteInitializers(invocationContext);

                    if (!invocationContext.IsTerminating)
                        scope.Complete();
                } // dispose scope
            }
            catch (Exception ex)
            {
                invocationContext.AddException(this, ex);
            }

            if (!invocationContext.IsTerminating)
                return;

            if (round >= FinalizerRetryCount)
            {
                InvocationContext.TakeExceptions(invocationContext);
            }
        }
    }

    private void CreateAndExecuteInitializers(ProcessInvocationContext invocationContext)
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
                process.Execute(this, invocationContext);

                if (invocationContext.IsTerminating)
                    break;
            }
        }
    }
}