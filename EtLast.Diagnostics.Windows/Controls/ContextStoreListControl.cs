namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Drawing;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextStoreListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public AbstractDiagContext Context { get; }
        public ObjectListView ListView { get; }

        private readonly ControlUpdater<TrackedStore> _updater;

        public ContextStoreListControl(Control container, AbstractDiagContext context)
        {
            Container = container;
            Context = context;

            _updater = new ControlUpdater<TrackedStore>(context, container)
            {
                ItemFilter = ItemFilter,
                AutoUpdateUntilContextLoaded = true,
            };

            _updater.CreateSearchBox(10, 10);

            ListView = ListViewHelpers.CreateListView(container);
            ListView.BorderStyle = BorderStyle.None;
            ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
            ListView.ItemActivate += ListView_ItemActivate;

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Rows",
                AspectGetter = x => (x as TrackedStore)?.RowCount,
                AspectToStringConverter = x => ((int?)x)?.FormatToStringNoZero(),
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Location",
                AspectGetter = x => (x as TrackedStore)?.Location,
            });

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Path",
                AspectGetter = x => (x as TrackedStore)?.Path,
            });

            context.WholePlaybook.OnRowStoreStarted += OnRowStoreStarted;

            _updater.ListView = ListView;
            _updater.Start();
        }

        private void ListView_ItemActivate(object sender, EventArgs e)
        {
            if (ListView.GetItem(ListView.SelectedIndex).RowObject is TrackedStore store)
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                var form = new Form()
#pragma warning restore CA2000 // Dispose objects before losing scope
                {
                    FormBorderStyle = FormBorderStyle.SizableToolWindow,
                    Text = "Store: " + store.Location + (!string.IsNullOrEmpty(store.Path) ? " / " + store.Path : ""),
                    Bounds = new Rectangle(Screen.PrimaryScreen.Bounds.Left + 100, Screen.PrimaryScreen.Bounds.Top + 100, Screen.PrimaryScreen.Bounds.Width - 200, Screen.PrimaryScreen.Bounds.Height - 200),
                    WindowState = FormWindowState.Maximized
                };

                var control = new RowStoreControl(form, Context, store);
                control.Refresh();

                form.ShowDialog();
            }
        }

        private bool ItemFilter(TrackedStore store)
        {
            return true;
        }

        private void OnRowStoreStarted(Playbook playbook, TrackedStore store)
        {
            _updater.AddItem(store);
        }
    }
}