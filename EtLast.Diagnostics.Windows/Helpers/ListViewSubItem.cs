namespace FizzCode.EtLast.Diagnostics.Windows;

public static class ListViewSubItemHelpers
{
    public static void SetIfChanged(this ListViewItem.ListViewSubItem item, string value)
    {
        if (item.Text != value)
        {
            item.Text = value;
        }
    }
}
