namespace FizzCode.EtLast
{
    using System;

    public interface IBasicScope : IExecutable
    {
        TransactionScopeKind TransactionScopeKind { get; set; }
        TransactionScopeKind CreationTransactionScopeKind { get; set; }

        bool StopOnError { get; set; }
        EventHandler<BasicScopeProcessFailedEventArgs> OnError { get; set; }
    }
}