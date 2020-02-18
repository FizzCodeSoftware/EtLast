namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Collections;
    using System.Windows.Forms;
    using BrightIdeasSoftware;

    public class CustomFilterMenuBuilder : FilterMenuBuilder
    {
        public override ToolStripDropDown MakeFilterMenu(ToolStripDropDown strip, ObjectListView listView, OLVColumn column)
        {
            if (!column.UseFiltering || column.ClusteringStrategy == null || listView.Objects == null)
                return strip;

            var clusters = Cluster(column.ClusteringStrategy, listView, column);
            if (clusters.Count > 0)
            {
                SortClusters(column.ClusteringStrategy, clusters);
                var menuItem = CreateFilteringMenuItem(column, clusters);

#pragma warning disable CA2000 // Dispose objects before losing scope
                var freeText = new ToolStripTextBoxWithPlaceHolderText()
                {
                    Tag = column,
                    Text = "",
                    PlaceHolderText = "text search...",
                };
#pragma warning restore CA2000 // Dispose objects before losing scope

                menuItem.DropDownItems.Insert(0, freeText);

                strip.Items.Add(menuItem);
            }

            return strip;
        }

        protected override void EnactFilter(ToolStripCheckedListBox checkedList, OLVColumn column)
        {
            if (!(column.ListView is ObjectListView olv) || olv.IsDisposed)
                return;

            var textBox = (checkedList.GetCurrentParent() as ToolStripDropDownMenu).Items[0] as ToolStripTextBox;
            if (!string.IsNullOrWhiteSpace(textBox.Text))
            {
                var filter = TextMatchFilter.Contains(olv, textBox.Text);
                filter.Columns = new[] { column };
                column.ValueBasedFilter = filter;
                column.ValuesChosenForFiltering = new ArrayList();
            }
            else
            {
                base.EnactFilter(checkedList, column);
            }
        }

        protected override void ClearAllFilters(OLVColumn column)
        {
            if (!(column.ListView is ObjectListView olv) || olv.IsDisposed)
                return;

            column.ValueBasedFilter = null;
            base.ClearAllFilters(column);
        }
    }
}