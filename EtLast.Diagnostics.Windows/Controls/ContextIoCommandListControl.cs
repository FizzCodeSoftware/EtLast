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

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextIoCommandListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public AbstractDiagContext Context { get; }
        public CheckBox ShowDbTransactionKind { get; }
        public CheckBox ShowDbConnectionKind { get; }
        public CheckBox HideVeryFast { get; }
        public Color HighlightedProcessForeColor { get; set; } = Color.Black;
        public Color HighlightedProcessBackColor { get; set; } = Color.FromArgb(150, 255, 255);
        public ContextProcessInvocationListControl LinkedProcessInvocationList { get; set; }

        private TrackedProcessInvocation _highlightedProcess;

        private readonly ControlUpdater<IoCommandModel> _updater;
        private readonly Dictionary<int, IoCommandModel> _itemByUid = new Dictionary<int, IoCommandModel>();

        public TrackedProcessInvocation HighlightedProcess
        {
            get => _highlightedProcess;
            set
            {
                _highlightedProcess = value;
                _updater.RefreshItems(true);
            }
        }

        public ContextIoCommandListControl(Control container, AbstractDiagContext context)
        {
            Container = container;
            Context = context;

            _updater = new ControlUpdater<IoCommandModel>(context, container)
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
                Checked = false,
            };

            HideVeryFast.CheckedChanged += (s, a) => _updater.RefreshItems(true);

            _updater.ListView.BorderStyle = BorderStyle.None;
            _updater.ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            _updater.ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);

            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "ID",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Uid,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timestamp",
                AspectGetter = x => (x as IoCommandModel)?.Timestamp,
                AspectToStringConverter = x => ((DateTime)x).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Context",
                AspectGetter = x => (x as IoCommandModel)?.Playbook.DiagContext.Name,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as IoCommandModel)?.Process.Name,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Kind",
                AspectGetter = x => (x as IoCommandModel)?.Process.KindToString(),
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Type",
                AspectGetter = x => (x as IoCommandModel)?.Process.Type,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Transaction",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.TransactionId,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Command kind",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Kind.ToString(),
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Location",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Location,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Path",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Path,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timeout",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.TimeoutSeconds,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Elapsed",
                AspectGetter = x => (x as IoCommandModel)?.Elapsed,
                AspectToStringConverter = x => x is TimeSpan ts ? FormattingHelpers.RightAlignedTimeSpanToString(ts) : null,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Affected data",
                AspectGetter = x => (x as IoCommandModel)?.EndEvent?.AffectedDataCount,
                AspectToStringConverter = x => x is int dc ? dc.FormatToString() : null,
                TextAlign = HorizontalAlignment.Right,
                HeaderTextAlign = HorizontalAlignment.Right,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Error",
                AspectGetter = x => (x as IoCommandModel)?.EndEvent?.ErrorMessage,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Arguments",
                AspectGetter = x => (x as IoCommandModel)?.ArgumentsPreview,
            });
            _updater.ListView.Columns.Add(new OLVColumn()
            {
                Text = "Command",
                AspectGetter = x => (x as IoCommandModel)?.StartEvent.Command,
            });

            _updater.ListView.ItemActivate += ListView_ItemActivate;
            _updater.ListView.FormatRow += ListView_FormatRow;

            _updater.Start();

            context.WholePlaybook.OnEventsAdded += OnEventsAdded;
        }

        private void ListView_ItemActivate(object sender, EventArgs e)
        {
            if (_updater.ListView.GetItem(_updater.ListView.SelectedIndex).RowObject is IoCommandModel item && item.Process != null)
            {
                LinkedProcessInvocationList?.SelectProcess(item.Process);
            }
        }

        private bool ItemFilter(IoCommandModel item)
        {
            if (HighlightedProcess == null)
                return true;

            if (item.Process.Topic != HighlightedProcess.Topic)
                return false;

            if (item.StartEvent.Kind == IoCommandKind.dbTransaction && !ShowDbTransactionKind.Checked)
                return false;

            if (item.StartEvent.Kind == IoCommandKind.dbConnection && !ShowDbConnectionKind.Checked)
                return false;

            if (item.Elapsed?.TotalMilliseconds < 10 && HideVeryFast.Checked)
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

                    _itemByUid.Add(startEvent.Uid, item);
                }
                else if (evt is IoCommandEndEvent endEvent)
                {
                    if (_itemByUid.TryGetValue(endEvent.Uid, out var item))
                    {
                        item.EndEvent = endEvent;
                        item.Elapsed = new TimeSpan(endEvent.Timestamp - item.StartEvent.Timestamp);
                    }
                }
            }
        }

        private void ListView_FormatRow(object sender, FormatRowEventArgs e)
        {
            if (e.Model is IoCommandModel item && item.Process == HighlightedProcess)
            {
                e.Item.ForeColor = HighlightedProcessForeColor;
                e.Item.BackColor = HighlightedProcessBackColor;
            }
            else
            {
                e.Item.ForeColor = _updater.ListView.ForeColor;
                e.Item.BackColor = _updater.ListView.BackColor;
            }
        }
    }
}