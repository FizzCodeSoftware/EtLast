namespace FizzCode.EtLast.Diagnostics.Windows;

using static System.Windows.Forms.ListViewItem;

public static class ListViewSubItemHelpers
{
    public static void SetIfChanged(this ListViewSubItem item, string value)
    {
        if (item.Text != value)
        {
            item.Text = value;
        }
    }
}
