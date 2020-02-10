namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Windows.Forms;

    public static class ToolTipSingleton
    {
        private static readonly ToolTip _toolTip = new ToolTip()
        {
            ShowAlways = true,
            AutoPopDelay = 5000,
            InitialDelay = 0,
            ReshowDelay = 500,
            IsBalloon = true,
        };

        public static void Show(string text, Control control, int x, int y)
        {
            _toolTip.Show(text, control, x + 8, y + 8);
        }

        public static void Remove(Control control)
        {
            _toolTip.SetToolTip(control, "");
        }
    }
}
