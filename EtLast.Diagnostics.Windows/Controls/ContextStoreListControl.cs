namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Drawing;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal class ContextStoreListControl
    {
        public Control Container { get; }
        public DiagContext Context { get; }

        private readonly ControlUpdater<TrackedStore> _updater;

        public ContextStoreListControl(Control container, DiagContext context)
        {
            Container = container;
            Context = context;

            _updater = new ControlUpdater<TrackedStore>(context, container)
            {
                ItemFilter = ItemFilter,
                AutoUpdateUntilContextLoaded = true,
            };

            _updater.CreateSearchBox(10, 10);

            _updater.ListView.BorderStyle = BorderStyle.None;
            _updater.ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            _updater.ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
            _updater.ListView.ItemActivate += ListView_ItemActivate;

            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Rows",
                AspectGetter = x => (x as TrackedStore)?.RowCount,
                AspectToStringConverter = x => ((int?)x)?.FormatToStringNoZero(),
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });

            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Location",
                AspectGetter = x => (x as TrackedStore)?.Location,
            });

            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Path",
                AspectGetter = x => (x as TrackedStore)?.Path,
            });

            context.WholePlaybook.OnRowStoreStarted += OnRowStoreStarted;

            _updater.Start();
        }

        private void ListView_ItemActivate(object sender, EventArgs e)
        {
            if (_updater.ListView.GetItem(_updater.ListView.SelectedIndex).RowObject is TrackedStore store)
            {
                var form = new Form()
                {
                    FormBorderStyle = FormBorderStyle.SizableToolWindow,
                    Text = "Store: " + store.Location + (!string.IsNullOrEmpty(store.Path) ? " / " + store.Path : ""),
                    StartPosition = FormStartPosition.Manual,
                    Bounds = new Rectangle(Screen.PrimaryScreen.Bounds.Left + 100, Screen.PrimaryScreen.Bounds.Top + 100, Screen.PrimaryScreen.Bounds.Width - 200, Screen.PrimaryScreen.Bounds.Height - 200),
                    WindowState = FormWindowState.Maximized,
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