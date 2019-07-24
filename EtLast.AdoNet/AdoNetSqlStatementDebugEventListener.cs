using System;

namespace FizzCode.EtLast.AdoNet
{
    public static class AdoNetSqlStatementDebugEventListener
    {
        /// <summary>
        /// Disabled by default.
        /// </summary>
        public static bool Enabled { get; set; }

        public static EventHandler<AdoNetSqlStatementDebugEvent> OnEvent { get; set; }

        internal static void GenerateEvent(IProcess process, Func<AdoNetSqlStatementDebugEvent> eventGenerator)
        {
            if (!Enabled)
                return;

            var e = eventGenerator.Invoke();
            if (e != null)
            {
                OnEvent?.Invoke(process, e);
            }
        }
    }
}