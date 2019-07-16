namespace FizzCode.EtLast
{
    using System;

    public interface IEtlStrategy
    {
        void Execute(IEtlContext context, TimeSpan transactionScopeTimeout);
    }
}