using System.Drawing;
using System.Windows.Forms;
using BrightIdeasSoftware;

namespace FizzCode.EtLast.Diagnostics.Windows
{
    internal static class ListViewHelpers
    {
        public static ObjectListView CreateListView(Control container)
        {
            return new FastObjectListView()
            {
                Parent = container,
                BorderStyle = BorderStyle.FixedSingle,
                ShowItemToolTips = true,
                ShowGroups = false,
                UseFiltering = true,
                ShowCommandMenuOnRightClick = true,
                SelectColumnsOnRightClickBehaviour = ObjectListView.ColumnSelectBehaviour.None,
                ShowFilterMenuOnRightClick = true,
                FullRowSelect = true,
                UseAlternatingBackColors = true,
                HeaderUsesThemes = true,
                GridLines = true,
                AlternateRowBackColor = Color.FromArgb(240, 240, 240),
                UseFilterIndicator = true,
                FilterMenuBuildStrategy = new CustomFilterMenuBuilder()
                {
                    MaxObjectsToConsider = int.MaxValue,
                },
                MultiSelect = false,
                HideSelection = false,
            };
        }
    }
}
