namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Runtime.InteropServices;

    internal class EnableVirtualTerminalProcessingHack
    {
        public void ApplyHack()
        {
            var stdOut = GetStdHandle(-11);
            if (stdOut != (IntPtr)(-1) && GetConsoleMode(stdOut, out var mode))
            {
                SetConsoleMode(stdOut, mode | 0x4);
            }
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetStdHandle(int handleId);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetConsoleMode(IntPtr handle, out uint mode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetConsoleMode(IntPtr handle, uint mode);
    }
}