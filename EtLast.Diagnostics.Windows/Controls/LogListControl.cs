﻿namespace FizzCode.EtLast.Diagnostics.Windows;

internal delegate void LogActionDelegate(LogModel logModel);

internal class LogListControl
{
    public Control Container { get; }
    public DiagContext Context { get; }
    public CheckBox ShowDebugLevel { get; }
    public LogActionDelegate OnLogDoubleClicked { get; set; }

    private readonly ControlUpdater<LogModel> _updater;

    public LogListControl(Control container, DiagContext context)
    {
        Container = container;
        Context = context;

        _updater = new ControlUpdater<LogModel>(null, Container)
        {
            ItemFilter = ItemFilter,
        };

        _updater.CreateSearchBox(10, 10);

        ShowDebugLevel = new CheckBox()
        {
            Parent = container,
            Bounds = new Rectangle(_updater.SearchBox.Right + 20, _updater.SearchBox.Top, 200, _updater.SearchBox.Height),
            Text = "Show debug level",
            CheckAlign = ContentAlignment.MiddleLeft,
        };

        ShowDebugLevel.CheckedChanged += (s, a) => _updater.RefreshItems(true);

        _updater.ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
        _updater.ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
        _updater.ListView.ItemActivate += ListView_ItemActivate;

        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Timestamp",
            AspectGetter = x => new DateTime((x as LogModel)?.Event.Timestamp ?? 0L),
            AspectToStringConverter = x => ((DateTime)x).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
        });
        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Severity",
            AspectGetter = x => (x as LogModel)?.Event.Severity.ToShortString(),
        });
        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Process",
            AspectGetter = x => (x as LogModel)?.Process?.Name,
        });
        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Kind",
            AspectGetter = x => (x as LogModel)?.Process?.KindToString(),
        });
        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Type",
            AspectGetter = x => (x as LogModel)?.Process?.Type,
        });
        _updater.ListView.Columns.Add(new OLVColumn()
        {
            Text = "Message",
            AspectGetter = x => (x as LogModel)?.Text,
        });

        _updater.Start();

        context.WholePlaybook.OnEventsAdded += OnEventsAdded;
    }

    private void ListView_ItemActivate(object sender, EventArgs e)
    {
        if (_updater.ListView.GetItem(_updater.ListView.SelectedIndex).RowObject is LogModel item)
        {
            OnLogDoubleClicked?.Invoke(item);
        }
    }

    private bool ItemFilter(LogModel item)
    {
        return item.Event.Severity > LogSeverity.Debug || ShowDebugLevel.Checked;
    }

    private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
    {
        var events = abstractEvents.OfType<LogEvent>().ToList();
        if (events.Count == 0)
            return;

        foreach (var evt in events)
        {
            var text = evt.Text;
            if (evt.Arguments != null)
            {
                foreach (var arg in evt.Arguments)
                {
                    text = arg.Key.StartsWith("{spacing", StringComparison.OrdinalIgnoreCase)
                        ? text.Replace(arg.Key, "", StringComparison.InvariantCultureIgnoreCase)
                        : text.Replace(arg.Key, FormattingHelpers.ToDisplayValue(arg.Value), StringComparison.InvariantCultureIgnoreCase);
                }
            }

            var item = new LogModel()
            {
                Event = evt,
                Process = evt.ProcessId != null
                    ? playbook.DiagContext.WholePlaybook.ProcessList[evt.ProcessId.Value]
                    : null,
                Text = text,
            };

            _updater.AddItem(item);
        }
    }
}

internal class LogModel
{
    public TrackedProcess Process { get; set; }
    public LogEvent Event { get; set; }
    public string Text { get; set; }
}
