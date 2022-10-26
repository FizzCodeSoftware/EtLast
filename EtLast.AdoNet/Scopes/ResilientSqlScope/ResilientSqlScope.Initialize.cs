namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private void Initialize(Pipe pipe)
    {
        if (Initializers == null)
            return;

        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (Pipe.IsTerminating)
                return;

            Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", round);
            try
            {
                using (var scope = Context.BeginTransactionScope(this, InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecuteInitializers(pipe);

                    if (!pipe.IsTerminating)
                        scope.Complete();
                } // dispose scope
            }
            catch (Exception ex)
            {
                pipe.AddException(this, ex);
            }

            if (!pipe.IsTerminating)
                return;

            if (round >= FinalizerRetryCount)
            {
                Pipe.TakeExceptions(pipe);
            }
        }
    }

    private void CreateAndExecuteInitializers(Pipe pipe)
    {
        IProcess[] initializers;

        using (var creatorScope = Context.BeginTransactionScope(this, TransactionScopeKind.Suppress, LogSeverity.Information))
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
                process.Execute(this, pipe);

                if (pipe.IsTerminating)
                    break;
            }
        }
    }
}