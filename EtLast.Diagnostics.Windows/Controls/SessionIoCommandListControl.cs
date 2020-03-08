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
        public CheckBox ShowDbTransactionKind { get; }
        public CheckBox ShowDbConnectionKind { get; }
        public CheckBox HideVeryFast { get; }
        public IoCommandActionDelegate OnIoCommandDoubleClicked { get; set; }

        private readonly ControlUpdater<IoCommandModel> _updater;
        private readonly Dictionary<string, Dictionary<int, IoCommandModel>> _itemByUid = new Dictionary<string, Dictionary<int, IoCommandModel>>();

        public SessionIoCommandListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, DiagSession session)
        {
            Container = container;
            Session = session;

            _updater = new ControlUpdater<IoCommandModel>(null, container)
            {
                ItemFilter = ItemFilter,
            };

            _updater.CreateSearchBox(10, 10);

            ShowDbTransactionKind = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(_updater.SearchBox.Right + 20, _updater.SearchBox.Top, 130, _updater.SearchBox.Height),
                Text = "DB transactions",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowDbTransactionKind.CheckedChanged += (s, a) => _updater.RefreshItems(true);

            ShowDbConnectionKind = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(ShowDbTransactionKind.Right + 20, ShowDbTransactionKind.Top, 130, ShowDbTransactionKind.Height),
                Text = "DB connections",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowDbConnectionKind.CheckedChanged += (s, a) => _updater.RefreshItems(true);

            HideVeryFast = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(ShowDbConnectionKind.Right + 20, ShowDbTransactionKind.Top, 130, ShowDbTransactionKind.Height),
                Text = "Hide very fast",
                CheckAlign = ContentAlignment.MiddleLeft,
                Checked = true,
            };

            HideVeryFast.CheckedChanged += (s, a) => _updater.RefreshItems(true);

            ListView = ListViewHelpers.CreateListView(container);
            ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
            ListView.ItemActivate += ListView_ItemActivate;

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "ID",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Uid,
            });
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
                Text = "Location",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Location,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Path",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Path,
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
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Command,
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
            if (ListView.GetItem(ListView.SelectedIndex).RowObject is IoCommandModel item)
            {
                OnIoCommandDoubleClicked?.Invoke(item);
            }
        }

        private bool ItemFilter(IoCommandModel item)
        {
            if (item.StartEvent.Kind == IoCommandKind.dbTransaction && !ShowDbTransactionKind.Checked)
                return false;

            if (item.StartEvent.Kind == IoCommandKind.dbConnection && !ShowDbConnectionKind.Checked)
                return false;

            if (item.Elapsed?.TotalMilliseconds < 100 && HideVeryFast.Checked)
                return false;

            return true;
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            var events = abstractEvents.OfType<IoCommandEvent>().ToList();
            if (events.Count == 0)
                return;

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
                        ArgumentsPreview = startEvent.Arguments != null
                            ? string.Join(",", startEvent.Arguments.Where(x => !x.Value.GetType().IsArray).Select(x => x.Key + "=" + FormattingHelpers.ToDisplayValue(x.Value)))
                            : null,
                    };

                    _updater.AddItem(item);

                    if (!_itemByUid.TryGetValue(playbook.DiagContext.Name, out var itemListByContext))
                    {
                        itemListByContext = new Dictionary<int, IoCommandModel>();
                        _itemByUid.Add(playbook.DiagContext.Name, itemListByContext);
                    }

                    itemListByContext.Add(startEvent.Uid, item);
                }
                else if (evt is IoCommandEndEvent endEvent)
                {
                    if (_itemByUid.TryGetValue(playbook.DiagContext.Name, out var itemListByContext))
                    {
                        if (itemListByContext.TryGetValue(endEvent.Uid, out var item))
                        {
                            item.EndEvent = endEvent;
                            item.Elapsed = new TimeSpan(endEvent.Timestamp - item.StartEvent.Timestamp);
                        }
                    }
                }
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
        public string ArgumentsPreview { get; set; }
    }
}