namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Windows.Forms;

    internal static class Program
    {
        [STAThread]
        private static void Main()
        {
            Application.SetHighDpiMode(HighDpiMode.SystemAware);
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            using (var mainForm = new MainForm())
            {
                Application.Run(mainForm);
            }
        }
    }
}