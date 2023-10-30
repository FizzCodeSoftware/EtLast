namespace FizzCode.EtLast;

public sealed partial class ResilientSqlScope : AbstractJob, IScope
{
    private void InitializeScope()
    {
        if (Initializers == null)
            return;

        var flowState = new FlowState(Context);
        for (var round = 0; round <= FinalizerRetryCount; round++)
        {
            if (flowState.IsTerminating)
                return;

            Context.Log(LogSeverity.Information, this, "initialization round {InitializationRound} started", round);
            try
            {
                using (var scope = Context.BeginTransactionScope(this, InitializationTransactionScopeKind, LogSeverity.Information))
                {
                    CreateAndExecuteInitializers(flowState);

                    if (!flowState.IsTerminating)
                        scope.Complete();
                } // dispose scope
            }
            catch (Exception ex)
            {
                flowState.AddException(this, ex);
            }

            if (!flowState.IsTerminating)
                return;

            if (round >= FinalizerRetryCount)
            {
                flowState.TakeExceptions(flowState);
            }
        }
    }

    private void CreateAndExecuteInitializers(FlowState flowState)
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
                process.Execute(this, flowState);

                if (flowState.IsTerminating)
                    break;
            }
        }
    }
}