namespace FizzCode.EtLast.Diagnostics.Windows
{
    using static System.Windows.Forms.ListViewItem;

    public static class ListViewSubItemHelpers
    {
        public static void SetIfNotChanged(this ListViewSubItem item, string value)
        {
            if (item.Text != value)
                item.Text = value;
        }
    }
}
