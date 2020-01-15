namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Drawing;
    using System.Linq;
    using System.Windows.Forms;

    internal static class FormExtensions
    {
        public static void MaximizeOnSecondaryScreen(this Form form)
        {
            var screen = Screen.AllScreens.Length == 1
                ? Screen.AllScreens[0]
                : Screen.AllScreens
            .Where(x => !x.Primary)
            .OrderByDescending(x => x.Bounds.Width)
            .First();

            form.SuspendLayout();
            form.Bounds = new Rectangle(screen.Bounds.Left, screen.Bounds.Top, form.Bounds.Width, form.Bounds.Height);
            form.WindowState = FormWindowState.Maximized;
            form.ResumeLayout();
        }
    }
}
