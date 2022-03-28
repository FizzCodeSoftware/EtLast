namespace FizzCode.EtLast.Diagnostics.Windows;

public class MyListView : ListView
{
    protected override void WndProc(ref Message m)
    {
        if (m.Msg == 0x203)
        {
            var x = (short)m.LParam;
            var y = (short)((int)m.LParam >> 16);
            var e = new MouseEventArgs(MouseButtons.Left, 2, x, y, 0);
            OnMouseDoubleClick(e);
        }

        base.WndProc(ref m);
    }
}
