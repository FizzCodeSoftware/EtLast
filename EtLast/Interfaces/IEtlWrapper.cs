namespace FizzCode.EtLast
{
    using System;

    public interface IEtlWrapper
    {
        void Execute(IEtlContext context, TimeSpan transactionScopeTimeout);
    }
}