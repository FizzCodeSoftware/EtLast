using FizzCode.EtLast.Diagnostics.Windows;

Application.SetHighDpiMode(HighDpiMode.SystemAware);
Application.EnableVisualStyles();
Application.SetCompatibleTextRenderingDefault(false);

using (var mainForm = new MainForm())
{
    Application.Run(mainForm);
}
