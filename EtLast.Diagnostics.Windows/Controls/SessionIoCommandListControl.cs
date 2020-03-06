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

    internal delegate void IoCommandActionDelegate(IoCommandModel ioCommandModel);

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionIoCommandListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public DiagSession Session { get; }
        public ObjectListView ListView { get; }
        public TextBox SearchBox { get; }
        public Timer AutoSizeTimer { get; }
        public CheckBox ShowDbTransactionKind { get; }
        public CheckBox ShowDbConnectionKind { get; }
        public CheckBox HideVeryFast { get; }
        public IoCommandActionDelegate OnIoCommandDoubleClicked { get; set; }

        private bool _newData;
        private readonly List<IoCommandModel> _allItems = new List<IoCommandModel>();
        private readonly Dictionary<string, Dictionary<int, IoCommandModel>> ItemByUid = new Dictionary<string, Dictionary<int, IoCommandModel>>();

        public SessionIoCommandListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, DiagSession session)
        {
            Container = container;
            Session = session;

            SearchBox = new TextBox()
            {
                Parent = container,
                Bounds = new Rectangle(10, 10, 150, 20),
            };

            SearchBox.TextChanged += SearchBox_TextChanged;

            ShowDbTransactionKind = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(SearchBox.Right + 20, SearchBox.Top, 130, SearchBox.Height),
                Text = "DB transactions",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowDbTransactionKind.CheckedChanged += (s, a) => RefreshItems();

            ShowDbConnectionKind = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(ShowDbTransactionKind.Right + 20, SearchBox.Top, 130, SearchBox.Height),
                Text = "DB connections",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowDbConnectionKind.CheckedChanged += (s, a) => RefreshItems();

            HideVeryFast = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(ShowDbConnectionKind.Right + 20, SearchBox.Top, 130, SearchBox.Height),
                Text = "Hide very fast",
                CheckAlign = ContentAlignment.MiddleLeft,
                Checked = true,
            };

            HideVeryFast.CheckedChanged += (s, a) => RefreshItems();

            ListView = ListViewHelpers.CreateListView(container);
            ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
            ListView.ItemActivate += ListView_ItemActivate;

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timestamp",
                AspectGetter = x => (x as IoCommandModel)?.Timestamp,
                AspectToStringConverter = x => ((DateTime)x).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Context",
                AspectGetter = x => (x as IoCommandModel)?.Playbook.DiagContext.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as IoCommandModel)?.Process.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Kind",
                AspectGetter = x => (x as IoCommandModel)?.Process.KindToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Type",
                AspectGetter = x => (x as IoCommandModel)?.Process.Type,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Topic",
                AspectGetter = x => (x as IoCommandModel)?.Process.Topic,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Transaction",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.TransactionId,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Command kind",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Kind.ToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Target",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Location,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timeout",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.TimeoutSeconds,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Elapsed",
                AspectGetter = x => (x as IoCommandModel)?.Elapsed,
                AspectToStringConverter = x => x is TimeSpan ts ? FormattingHelpers.RightAlignedTimeSpanToString(ts) : null,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Affected data",
                AspectGetter = x => (x as IoCommandModel)?.EndEvent?.AffectedDataCount,
                AspectToStringConverter = x => x is int dc ? dc.FormatToString() : null,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Error",
                AspectGetter = x => (x as IoCommandModel)?.EndEvent?.ErrorMessage,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Arguments",
                AspectGetter = x => (x as IoCommandModel)?.ArgumentsPreview,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Command",
                AspectGetter = x => (x as IoCommandModel)?.CommandPreview,
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
            if (ListView.GetItem(ListView.SelectedIndex).RowObject is IoCommandModel item)
            {
                OnIoCommandDoubleClicked?.Invoke(item);
            }
        }

        private bool ItemVisible(IoCommandModel item)
        {
            if (item.StartEvent.Kind == IoCommandKind.dbTransaction && !ShowDbTransactionKind.Checked)
                return false;

            if (item.StartEvent.Kind == IoCommandKind.dbConnection && !ShowDbConnectionKind.Checked)
                return false;

            if (item.Elapsed?.TotalMilliseconds < 100 && HideVeryFast.Checked)
                return false;

            return true;
        }

        private void RefreshItems()
        {
            ListView.SetObjects(_allItems.Where(ItemVisible).OrderBy(x => x.Timestamp));
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
            var events = abstractEvents.OfType<IoCommandEvent>().ToList();
            if (events.Count == 0)
                return;

            var newItems = new List<IoCommandModel>();

            foreach (var evt in events)
            {
                if (evt is IoCommandStartEvent startEvent)
                {
                    var item = new IoCommandModel()
                    {
                        Timestamp = new DateTime(evt.Timestamp),
                        Playbook = playbook,
                        StartEvent = startEvent,
                        Process = playbook.DiagContext.WholePlaybook.ProcessList[startEvent.ProcessInvocationUid],
                        CommandPreview = startEvent.Command?
                            .Trim()
                            .Replace("\n", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Replace("\t", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Replace("  ", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Trim()
                            .MaxLengthWithEllipsis(300),
                        ArgumentsPreview = startEvent.Arguments != null
                            ? string.Join(",", startEvent.Arguments.Where(x => !x.Value.GetType().IsArray).Select(x => x.Key + "=" + FormattingHelpers.ToDisplayValue(x.Value)))
                            : null,
                    };

                    _allItems.Add(item);

                    if (!ItemByUid.TryGetValue(playbook.DiagContext.Name, out var itemListByContext))
                    {
                        itemListByContext = new Dictionary<int, IoCommandModel>();
                        ItemByUid.Add(playbook.DiagContext.Name, itemListByContext);
                    }

                    itemListByContext.Add(startEvent.Uid, item);
                    if (ItemVisible(item))
                    {
                        newItems.Add(item);
                    }
                }
                else if (evt is IoCommandEndEvent endEvent)
                {
                    if (ItemByUid.TryGetValue(playbook.DiagContext.Name, out var itemListByContext))
                    {
                        if (itemListByContext.TryGetValue(endEvent.Uid, out var item))
                        {
                            item.EndEvent = endEvent;
                            item.Elapsed = new TimeSpan(endEvent.Timestamp - item.StartEvent.Timestamp);
                            //ListView.RefreshObject(item);
                        }
                    }
                }
            }

            if (newItems.Count > 0)
            {
                //ListView.AddObjects(newItems);
                RefreshItems();
                _newData = true;
            }
        }
    }

    public class IoCommandModel
    {
        public DateTime Timestamp { get; set; }
        public Playbook Playbook { get; set; }
        public TrackedProcessInvocation Process { get; set; }
        public IoCommandStartEvent StartEvent { get; set; }
        public IoCommandEndEvent EndEvent { get; set; }
        public TimeSpan? Elapsed { get; set; }
        public string CommandPreview { get; set; }
        public string ArgumentsPreview { get; set; }
    }
}