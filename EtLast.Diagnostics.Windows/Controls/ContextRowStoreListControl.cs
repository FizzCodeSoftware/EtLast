namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextRowStoreListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public ListView ListView { get; }
        private readonly Dictionary<int, ListViewItem> _listItemByStoreUid = new Dictionary<int, ListViewItem>();
        private readonly System.Threading.Timer _statUpdateTimer;

        public ContextRowStoreListControl(Control container, AbstractDiagContext context)
        {
            Context = context;

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

            ListView.Columns.Add("rows", 60);
            ListView.Columns.Add("store", 1000);

            ListView.MouseDoubleClick += ListView_MouseDoubleClick;

            context.WholePlaybook.OnRowStoreStarted += OnRowStoreStarted;

            _statUpdateTimer = new System.Threading.Timer((state) => UpdateStats());
            _statUpdateTimer.Change(500, System.Threading.Timeout.Infinite);
        }

        private void OnRowStoreStarted(Playbook playbook, TrackedStore store)
        {
            ListView.BeginUpdate();

            try
            {
                var item = new ListViewItem("0")
                {
                    Tag = store,
                };

                item.SubItems.Add(string.Join(" / ", store.Descriptor.Select(kvp => kvp.Value)));

                ListView.Items.Add(item);
                _listItemByStoreUid.Add(store.UID, item);
            }
            finally
            {
                ListView.EndUpdate();
            }
        }

        private void UpdateStats()
        {
            _statUpdateTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            ListView.Invoke(new Action(() =>
            {
                var changed = false;
                foreach (ListViewItem item in ListView.Items)
                {
                    if (item.Tag is TrackedStore store)
                    {
                        if (item.SubItems[0].Text != store.RowCount.FormatToStringNoZero())
                        {
                            changed = true;
                            break;
                        }

                        changed = false;
                    }
                }

                if (changed)
                {
                    ListView.BeginUpdate();
                    try
                    {
                        foreach (ListViewItem item in ListView.Items)
                        {
                            if (item.Tag is TrackedStore store)
                            {
                                item.SubItems[0].SetIfChanged(store.RowCount.FormatToStringNoZero());
                            }
                        }
                    }
                    finally
                    {
                        ListView.EndUpdate();
                    }
                }
            }));

            if (!Context.FullyLoaded)
            {
                _statUpdateTimer.Change(500, System.Threading.Timeout.Infinite);
            }
        }

        private void ListView_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var info = list.HitTest(e.X, e.Y);

            if (info.Item?.Tag is TrackedStore store)
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                var form = new Form()
#pragma warning restore CA2000 // Dispose objects before losing scope
                {
                    FormBorderStyle = FormBorderStyle.SizableToolWindow,
                    Text = "Store: " + string.Join(" / ", store.Descriptor.Select(kvp => kvp.Value)),
                    Bounds = new System.Drawing.Rectangle(Screen.PrimaryScreen.Bounds.Left + 100, Screen.PrimaryScreen.Bounds.Top + 100, Screen.PrimaryScreen.Bounds.Width - 200, Screen.PrimaryScreen.Bounds.Height - 200),
                    WindowState = FormWindowState.Maximized
                };

                var control = new ContextRowStoreControl(form, Context, store);
                control.Refresh();

                form.ShowDialog();
            }
        }
    }
}