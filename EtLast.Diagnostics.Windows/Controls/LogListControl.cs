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
        public TextBox SearchBox { get; }
        public Timer AutoSizeTimer { get; }
        public CheckBox ShowDebugLevel { get; }
        public LogActionDelegate OnLogDoubleClicked { get; set; }

        private readonly List<LogModel> _allItems = new List<LogModel>();
        private bool _newData;

        public LogListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, DiagSession session)
        {
            Container = container;
            Session = session;

            SearchBox = new TextBox()
            {
                Parent = container,
                Bounds = new Rectangle(10, 10, 150, 20),
            };

            SearchBox.TextChanged += SearchBox_TextChanged;

            ShowDebugLevel = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(SearchBox.Right + 20, SearchBox.Top, 200, SearchBox.Height),
                Text = "Show debug level",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowDebugLevel.CheckedChanged += (s, a) => RefreshItems();

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

            AutoSizeTimer = new Timer()
            {
                Interval = 1000,
                Enabled = true,
            };

            AutoSizeTimer.Tick += AutoSizeTimer_Tick;

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

        private bool ItemVisible(LogModel item)
        {
#pragma warning disable RCS1073 // Convert 'if' to 'return' statement.
            if (item.Event.Severity <= LogSeverity.Debug && !ShowDebugLevel.Checked)
                return false;
#pragma warning restore RCS1073 // Convert 'if' to 'return' statement.

            return true;
        }

        private void RefreshItems()
        {
            ListView.SetObjects(_allItems.Where(ItemVisible).OrderBy(x => x.Event.Timestamp));
        }

        private void AutoSizeTimer_Tick(object sender, EventArgs e)
        {
            if (!_newData || !ListView.Visible)
                return;

            _newData = false;

            ListView.BeginUpdate();
            try
            {
                foreach (OLVColumn col in ListView.Columns)
                {
                    col.MinimumWidth = 0;
                    col.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
                }

                foreach (OLVColumn col in ListView.Columns)
                {
                    col.Width += 20;
                }
            }
            finally
            {
                ListView.EndUpdate();
            }
        }

        private void SearchBox_TextChanged(object sender, EventArgs e)
        {
            var text = (sender as TextBox).Text;
            ListView.AdditionalFilter = !string.IsNullOrEmpty(text)
                ? TextMatchFilter.Contains(ListView, text)
                : null;
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            var events = abstractEvents.OfType<LogEvent>().ToList();
            if (events.Count == 0)
                return;

            var newItems = new List<LogModel>();

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

                _allItems.Add(item);

                if (ItemVisible(item))
                {
                    newItems.Add(item);
                }
            }

            if (newItems.Count > 0)
            {
                RefreshItems();
                //ListView.AddObjects(newItems);
                _newData = true;
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