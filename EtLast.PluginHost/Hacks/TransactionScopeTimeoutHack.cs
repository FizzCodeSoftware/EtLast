namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Reflection;

    internal static class TransactionScopeTimeoutHack
    {
        /// <summary>
        /// Enable transaction timeout more than 10 mins.
        /// </summary>
        public static void ApplyHack(TimeSpan newMaxTimeout)
        {
            var type = typeof(System.Transactions.TransactionManager);
            var cachedMaxTimeout = type.GetField("_cachedMaxTimeout", BindingFlags.NonPublic | BindingFlags.Static);
            var maximumTimeout = type.GetField("_maximumTimeout", BindingFlags.NonPublic | BindingFlags.Static);

            cachedMaxTimeout.SetValue(null, true);
            maximumTimeout.SetValue(null, newMaxTimeout);
        }
    }
}