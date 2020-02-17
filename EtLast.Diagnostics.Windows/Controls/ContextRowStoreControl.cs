namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Collections.Generic;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextRowStoreControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractExecutionContext Context { get; }
        public TrackedStore Store { get; }
        public ListView ListView { get; }
        private readonly Dictionary<string, int> _columnIndexes = new Dictionary<string, int>();

        public ContextRowStoreControl(Control container, AbstractExecutionContext context, TrackedStore store)
        {
            Context = context;
            Store = store;

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = true,
                GridLines = true,
                AllowColumnReorder = false,
                FullRowSelect = true,
                BorderStyle = BorderStyle.FixedSingle,
                Width = 250,
                ShowItemToolTips = true,
            };

            ListView.Columns.Add("UID", 60);
            ListView.Columns.Add("Process", 150);
        }

        public void Refresh()
        {
            ListView.BeginUpdate();
            try
            {
                ListView.Items.Clear();

                Context.EnumerateThroughStoredRows(Store.UID, evt =>
                {
                    if (!Context.WholePlaybook.ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                        return;

                    var item = ListView.Items.Add(evt.RowUid.FormatToString());
                    item.SubItems.Add(process.Name);

                    foreach (var kvp in evt.Values)
                    {
                        if (!_columnIndexes.TryGetValue(kvp.Key, out var columnIndex))
                        {
                            ListView.Columns.Add(kvp.Key, 150);
                            columnIndex = ListView.Columns.Count - 1;
                            _columnIndexes.Add(kvp.Key, columnIndex);
                        }

                        while (item.SubItems.Count <= columnIndex)
                            item.SubItems.Add("NULL");

                        item.SubItems[columnIndex].Text = FormattingHelpers.ToDisplayValue(kvp.Value)
                            + (kvp.Value == null
                                ? null
                                : " (" + kvp.Value.GetType().GetFriendlyTypeName() + ")");
                    }
                });

                //ListView.AutoResizeColumns(ColumnHeaderAutoResizeStyle.ColumnContent);
            }
            finally
            {
                ListView.EndUpdate();
            }
        }
    }
}