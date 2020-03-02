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
    internal class SessionIoCommandListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public DiagSession Session { get; }
        public ObjectListView ListView { get; }
        public TextBox SearchBox { get; }
        public Timer AutoSizeTimer { get; }
        public CheckBox ShowTransactionKind { get; }
        private bool _newData;
        private readonly List<Model> AllItems = new List<Model>();
        private readonly Dictionary<string, Dictionary<int, Model>> ItemByUid = new Dictionary<string, Dictionary<int, Model>>();

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

            ShowTransactionKind = new CheckBox()
            {
                Parent = container,
                Bounds = new Rectangle(SearchBox.Right + 20, SearchBox.Top, 200, SearchBox.Height),
                Text = "Show transaction commands",
                CheckAlign = ContentAlignment.MiddleLeft,
            };

            ShowTransactionKind.CheckedChanged += (s, a) => RefreshItems();

            ListView = ListViewHelpers.CreateListView(container);
            ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
            ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);

            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timestamp",
                AspectGetter = x => (x as Model)?.Timestamp,
                AspectToStringConverter = x => ((DateTime)x).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Context",
                AspectGetter = x => (x as Model)?.Playbook.DiagContext.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Topic",
                AspectGetter = x => (x as Model)?.Process.Topic,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as Model)?.Process.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Kind",
                AspectGetter = x => (x as Model)?.Process.KindToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process type",
                AspectGetter = x => (x as Model)?.Process.ShortType,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Transaction",
                AspectGetter = x => (x as Model)?.StartEvent.TransactionId,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Comm. kind",
                AspectGetter = x => (x as Model)?.StartEvent.Kind.ToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Target",
                AspectGetter = x => (x as Model)?.StartEvent.Location,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Timeout",
                AspectGetter = x => (x as Model)?.StartEvent.TimeoutSeconds,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Elapsed",
                AspectGetter = x => (x as Model)?.EndEvent == null ? (TimeSpan?)null : new TimeSpan((x as Model).EndEvent.Timestamp - (x as Model).StartEvent.Timestamp),
                AspectToStringConverter = x => x is TimeSpan ts ? FormattingHelpers.TimeSpanToString(ts) : null,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Affected data",
                AspectGetter = x => (x as Model)?.EndEvent?.AffectedDataCount,
                AspectToStringConverter = x => x is int dc ? dc.FormatToStringNoZero() : null,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Error",
                AspectGetter = x => (x as Model)?.EndEvent?.ErrorMessage,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Arguments",
                AspectGetter = x => (x as Model)?.ArgumentsPreview,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Command",
                AspectGetter = x => (x as Model)?.CommandPreview,
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

        private bool ItemVisible(Model item)
        {
#pragma warning disable RCS1073 // Convert 'if' to 'return' statement.
            if (item.StartEvent.Kind == IoCommandKind.dbTransaction && !ShowTransactionKind.Checked)
                return false;
#pragma warning restore RCS1073 // Convert 'if' to 'return' statement.

            return true;
        }

        private void RefreshItems()
        {
            ListView.SetObjects(AllItems.Where(ItemVisible));
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
            var eventsQuery = abstractEvents.OfType<IoCommandEvent>();

            var events = eventsQuery.ToList();
            if (events.Count == 0)
                return;

            ListView.BeginUpdate();
            try
            {
                var newItems = new List<Model>();

                foreach (var evt in events)
                {
                    if (evt is IoCommandStartEvent startEvent)
                    {
                        var item = new Model()
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

                        AllItems.Add(item);

                        if (!ItemByUid.TryGetValue(playbook.DiagContext.Name, out var itemListByContext))
                        {
                            itemListByContext = new Dictionary<int, Model>();
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
                                //ListView.RefreshObject(item);
                            }
                        }
                    }
                }

                ListView.AddObjects(newItems);
                _newData = true;
            }
            finally
            {
                ListView.EndUpdate();
            }
        }

        private class Model
        {
            public DateTime Timestamp { get; set; }
            public Playbook Playbook { get; set; }
            public TrackedProcessInvocation Process { get; set; }
            public IoCommandStartEvent StartEvent { get; set; }
            public IoCommandEndEvent EndEvent { get; set; }
            public string CommandPreview { get; set; }
            public string ArgumentsPreview { get; set; }
        }
    }
}