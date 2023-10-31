namespace FizzCode.EtLast.Diagnostics.Windows;

internal class ContextSinkListControl
{
    public Control Container { get; }
    public DiagContext Context { get; }

    private readonly ControlUpdater<TrackedSink> _updater;

    public ContextSinkListControl(Control container, DiagContext context)
    {
        Container = container;
        Context = context;

        _updater = new ControlUpdater<TrackedSink>(context, container)
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
            AspectGetter = x => (x as TrackedSink)?.RowCount,
            AspectToStringConverter = x => ((long?)x)?.FormatToStringNoZero(),
            TextAlign = HorizontalAlignment.Right,
            HeaderTextAlign = HorizontalAlignment.Right,
        });

        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Location",
            AspectGetter = x => (x as TrackedSink)?.Location,
        });

        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Path",
            AspectGetter = x => (x as TrackedSink)?.Path,
        });

        context.WholePlaybook.OnSinkStarted += OnSinkStarted;

        _updater.Start();
    }

    private void ListView_ItemActivate(object sender, EventArgs e)
    {
        if (_updater.ListView.GetItem(_updater.ListView.SelectedIndex).RowObject is TrackedSink sink)
        {
            var form = new Form()
            {
                FormBorderStyle = FormBorderStyle.SizableToolWindow,
                Text = "Sink: " + sink.Location + (!string.IsNullOrEmpty(sink.Path) ? " / " + sink.Path : ""),
                StartPosition = FormStartPosition.Manual,
                Bounds = new Rectangle(Screen.PrimaryScreen.Bounds.Left + 100, Screen.PrimaryScreen.Bounds.Top + 100, Screen.PrimaryScreen.Bounds.Width - 200, Screen.PrimaryScreen.Bounds.Height - 200),
                WindowState = FormWindowState.Maximized,
            };

            var control = new SinkControl(form, Context, sink);
            control.Refresh();

            form.ShowDialog();
        }
    }

    private bool ItemFilter(TrackedSink sink)
    {
        return true;
    }

    private void OnSinkStarted(Playbook playbook, TrackedSink sink)
    {
        _updater.AddItem(sink);
    }
}
