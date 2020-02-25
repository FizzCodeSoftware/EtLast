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
    internal class LogListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public DiagSession Session { get; }
        public ObjectListView ListView { get; }
        public TextBox SearchBox { get; }
        public Timer AutoSizeTimer { get; }
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
                Text = "Severity",
                AspectGetter = x => (x as Model)?.Event.Severity.ToShortString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Topic",
                AspectGetter = x => (x as Model)?.Process?.Topic,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Process",
                AspectGetter = x => (x as Model)?.Process?.Name,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Kind",
                AspectGetter = x => (x as Model)?.Process?.KindToString(),
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Type",
                AspectGetter = x => (x as Model)?.Process?.ShortType,
            });
            ListView.Columns.Add(new OLVColumn()
            {
                Text = "Text",
                AspectGetter = x => (x as Model)?.Text,
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

            ListView.BeginUpdate();
            try
            {
                var modelList = new List<Model>();

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

                    var model = new Model()
                    {
                        Timestamp = new DateTime(evt.Timestamp),
                        Playbook = playbook,
                        Event = evt,
                        Process = evt.ProcessInvocationUID != null
                            ? playbook.DiagContext.WholePlaybook.ProcessList[evt.ProcessInvocationUID.Value]
                            : null,
                        Text = text,
                    };

                    modelList.Add(model);
                }

                ListView.AddObjects(modelList);
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
            public LogEvent Event { get; set; }
            public string Text { get; set; }
        }
    }
}