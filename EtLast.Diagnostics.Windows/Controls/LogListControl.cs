namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal delegate void LogActionDelegate(LogModel logModel);

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class LogListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public DiagSession Session { get; }
        public ObjectListView ListView { get; }
        public CheckBox ShowDebugLevel { get; }
        public LogActionDelegate OnLogDoubleClicked { get; set; }

        private readonly ControlUpdater<LogModel> _updater;

        public LogListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, DiagSession session)
        {
            Container = container;
            Session = session;

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

            ListView = ListViewHelpers.CreateListView(container);
            ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
            ListView.ItemActivate += ListView_ItemActivate;

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timestamp",
                AspectGetter = x => (x as LogModel)?.Timestamp,
                AspectToStringConverter = x => ((DateTime)x).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Severity",
                AspectGetter = x => (x as LogModel)?.Event.Severity.ToShortString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Context",
                AspectGetter = x => (x as LogModel)?.Playbook.DiagContext.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as LogModel)?.Process?.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Kind",
                AspectGetter = x => (x as LogModel)?.Process?.KindToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Type",
                AspectGetter = x => (x as LogModel)?.Process?.Type,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Topic",
                AspectGetter = x => (x as LogModel)?.Process?.Topic,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Message",
                AspectGetter = x => (x as LogModel)?.Text,
            });

            _updater.ListView = ListView;
            _updater.Start();

            diagnosticsStateManager.OnDiagContextCreated += ec =>
            {
                if (ec.Session == session)
                {
                    ec.WholePlaybook.OnEventsAdded += OnEventsAdded;
                }
            };
        }

        private void ListView_ItemActivate(object sender, EventArgs e)
        {
            if (ListView.GetItem(ListView.SelectedIndex).RowObject is LogModel item)
            {
                OnLogDoubleClicked?.Invoke(item);
            }
        }

        private bool ItemFilter(LogModel item)
        {
#pragma warning disable RCS1073 // Convert 'if' to 'return' statement.
            if (item.Event.Severity <= LogSeverity.Debug && !ShowDebugLevel.Checked)
                return false;
#pragma warning restore RCS1073 // Convert 'if' to 'return' statement.

            return true;
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
                        text = text.Replace(arg.Key, FormattingHelpers.ToDisplayValue(arg.Value), StringComparison.InvariantCultureIgnoreCase);
                    }
                }

                var item = new LogModel()
                {
                    Timestamp = new DateTime(evt.Timestamp),
                    Playbook = playbook,
                    Event = evt,
                    Process = evt.ProcessInvocationUID != null
                        ? playbook.DiagContext.WholePlaybook.ProcessList[evt.ProcessInvocationUID.Value]
                        : null,
                    Text = text,
                };

                _updater.AddItem(item);
            }
        }
    }

    internal class LogModel
    {
        public DateTime Timestamp { get; set; }
        public Playbook Playbook { get; set; }
        public TrackedProcessInvocation Process { get; set; }
        public LogEvent Event { get; set; }
        public string Text { get; set; }
    }
}