namespace FizzCode.EtLast;

public enum TransactionScopeKind
{
    None = -1,
    //
    // Summary:
    //     A transaction is required by the scope. It uses an ambient transaction if one
    //     already exists. Otherwise, it creates a new transaction before entering the scope.
    //     This is the default value.
    Required = 0,
    //
    // Summary:
    //     A new transaction is always created for the scope.
    RequiresNew = 1,
    //
    // Summary:
    //     The ambient transaction context is suppressed when creating the scope. All operations
    //     within the scope are done without an ambient transaction context.
    Suppress = 2
}
