namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextDataStoreCommandListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public ListView ListView { get; }
        public Color HighlightedProcessForeColor { get; set; } = Color.Black;
        public Color HighlightedProcessBackColor { get; set; } = Color.FromArgb(150, 255, 255);
        public ContextProcessInvocationListControl LinkedProcessInvocationList { get; set; }

        private TrackedProcessInvocation _highlightedProcess;

        private readonly List<AbstractEvent> _allEvents = new List<AbstractEvent>();

        public TrackedProcessInvocation HighlightedProcess
        {
            get => _highlightedProcess;
            set
            {
                var topicChanged = _highlightedProcess?.Topic != value?.Topic;
                _highlightedProcess = value;
                UpdateHighlight(topicChanged);
            }
        }

        public ContextDataStoreCommandListControl(Control container, AbstractDiagContext context)
        {
            Context = context;

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = true,
                GridLines = true,
                AllowColumnReorder = false,
                FullRowSelect = true,
                BorderStyle = BorderStyle.FixedSingle,
            };

            ListView.Columns.Add("timestamp", 85);
            ListView.Columns.Add("process", 300);
            ListView.Columns.Add("kind", 60);
            ListView.Columns.Add("type", 300);
            ListView.Columns.Add("transaction", 85);
            ListView.Columns.Add("kind", 85);
            ListView.Columns.Add("location", 100);
            // todo: add timeout
            ListView.Columns.Add("command", 700);
            ListView.Columns.Add("arguments", 200);

            ListView.MouseDoubleClick += ListView_MouseDoubleClick;
        }

        private void ListView_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var info = list.HitTest(e.X, e.Y);

            if (info.SubItem?.Tag is TrackedProcessInvocation process)
            {
                LinkedProcessInvocationList?.SelectProcess(process);
            }
        }

        internal void ProcessNewEvents(List<AbstractEvent> abstractEvents, bool clear)
        {
            var unfilteredEvents = abstractEvents.OfType<DataStoreCommandStartEvent>().ToList();
            if (!clear)
            {
                _allEvents.AddRange(unfilteredEvents);
            }

            if (HighlightedProcess == null)
                return;

            var events = unfilteredEvents
                .Where(evt => Context.WholePlaybook.ProcessList[evt.ProcessInvocationUid].Topic == HighlightedProcess.Topic)
                .ToList();

            if (events.Count == 0)
                return;

            ListView.BeginUpdate();

            try
            {
                if (clear)
                {
                    ListView.Items.Clear();
                }

                foreach (var evt in events)
                {
                    var item = new ListViewItem(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture))
                    {
                        Tag = evt,
                    };

                    var process = Context.WholePlaybook.ProcessList[evt.ProcessInvocationUid];

                    item.ForeColor = HighlightedProcess != null && process == HighlightedProcess
                        ? HighlightedProcessForeColor
                        : ListView.ForeColor;

                    item.BackColor = HighlightedProcess != null && process == HighlightedProcess
                        ? HighlightedProcessBackColor
                        : ListView.BackColor;

                    item.SubItems.Add(process.Name).Tag = process;
                    item.SubItems.Add(process.KindToString());
                    item.SubItems.Add(process.ShortType);
                    item.SubItems.Add(evt.TransactionId);
                    item.SubItems.Add(evt.Kind.ToString());
                    item.SubItems.Add(evt.Location);
                    item.SubItems.Add(evt.Command
                        .Trim()
                        .Replace("\n", " ", StringComparison.InvariantCultureIgnoreCase)
                        .Replace("\t", " ", StringComparison.InvariantCultureIgnoreCase)
                        .Replace("  ", " ", StringComparison.InvariantCultureIgnoreCase)
                        .Trim()
                        .MaxLengthWithEllipsis(300));
                    item.SubItems.Add(evt.Arguments != null
                        ? string.Join(",", evt.Arguments.Where(x => !x.Value.GetType().IsArray).Select(x => x.Key + "=" + FormattingHelpers.ToDisplayValue(x.Value)))
                        : null);

                    ListView.Items.Add(item);
                }
            }
            finally
            {
                ListView.EndUpdate();
            }
        }

        private void UpdateHighlight(bool topicChanged)
        {
            if (topicChanged)
            {
                ProcessNewEvents(_allEvents, true);
            }
            else
            {
                ListView.BeginUpdate();

                try
                {
                    foreach (var item in ListView.Items.ToEnumerable<ListViewItem>())
                    {
                        var evt = item.Tag as DataStoreCommandStartEvent;
                        var process = Context.WholePlaybook.ProcessList[evt.ProcessInvocationUid];

                        item.ForeColor = HighlightedProcess != null && process == HighlightedProcess
                            ? HighlightedProcessForeColor
                            : ListView.ForeColor;

                        item.BackColor = HighlightedProcess != null && process == HighlightedProcess
                            ? HighlightedProcessBackColor
                            : ListView.BackColor;
                    }
                }
                finally
                {
                    ListView.EndUpdate();
                }
            }
        }
    }
}