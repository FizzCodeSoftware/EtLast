namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using static System.Windows.Forms.ListViewItem;

    public static class ListViewSubItemHelpers
    {
        public static void SetIfChanged(this ListViewSubItem item, string value, Func<string> tagCreator = null)
        {
            if (item.Text != value)
            {
                item.Text = value;
                item.Tag = tagCreator;
            }
        }
    }
}
