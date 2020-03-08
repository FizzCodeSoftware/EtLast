namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Windows.Forms;

    public class MyListView : ListView
    {
        protected override void WndProc(ref Message message)
        {
            if (message.Msg == 0x203)
            {
                var x = (short)message.LParam;
                var y = (short)((int)message.LParam >> 16);
                var e = new MouseEventArgs(MouseButtons.Left, 2, x, y, 0);
                OnMouseDoubleClick(e);
            }

            base.WndProc(ref message);
        }
    }
}