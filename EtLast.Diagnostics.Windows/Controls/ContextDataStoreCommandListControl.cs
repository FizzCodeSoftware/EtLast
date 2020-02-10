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
        public ExecutionContext Context { get; }
        public ListView ListView { get; }
        public Color HighlightedProcessForeColor { get; set; } = Color.White;
        public Color HighlightedTopicBackColor { get; set; } = Color.Red;
        public Color HighlightedTopicForeColor { get; set; } = Color.Black;

        private TrackedProcessInvocation _highlightedProcess;

        public TrackedProcessInvocation HighlightedProcess
        {
            get => _highlightedProcess;
            set
            {
                _highlightedProcess = value;
                UpdateHighlight();
            }
        }

        public ContextDataStoreCommandListControl(Control container, ExecutionContext context)
        {
            Context = context;

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = false,
                GridLines = false,
                AllowColumnReorder = false,
                FullRowSelect = true,
                BorderStyle = BorderStyle.FixedSingle,
            };

            ListView.Columns.Add("timestamp", 85);
            ListView.Columns.Add("topic", 300).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("process", 300);
            ListView.Columns.Add("process type", 300);
            ListView.Columns.Add("text", 700);
            ListView.Columns.Add("arguments", 200);
        }

        internal void ProcessNewDataStoreCommands(List<AbstractEvent> abstractEvents)
        {
            var eventsQuery = abstractEvents.OfType<DataStoreCommandEvent>();
            /*if (ProcessUidFilter != null)
                eventsQuery = eventsQuery.Where(x => x.ProcessUid == ProcessUidFilter.Value);*/

            var events = eventsQuery.ToList();
            if (events.Count == 0)
                return;

            ListView.Invoke((Action)delegate
            {
                ListView.BeginUpdate();
                try
                {
                    foreach (var evt in events)
                    {
                        var item = new ListViewItem(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture))
                        {
                            Tag = evt,
                        };

                        var process = Context.WholePlaybook.ProcessList[evt.ProcessInvocationUID];

                        item.ForeColor = HighlightedProcess != null && process == HighlightedProcess
                            ? HighlightedProcessForeColor
                            : HighlightedProcess != null && process.Topic == HighlightedProcess.Topic
                                ? HighlightedTopicForeColor
                                : ListView.ForeColor;

                        item.BackColor = HighlightedProcess != null && process.Topic == HighlightedProcess.Topic
                            ? HighlightedTopicBackColor
                            : ListView.BackColor;

                        item.SubItems.Add(process.Topic);
                        item.SubItems.Add(process.Name).Tag = process;
                        item.SubItems.Add(process.Type);
                        item.SubItems.Add(evt.Command
                            .Trim()
                            .Replace("\n", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Replace("\t", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Replace("  ", " ", StringComparison.InvariantCultureIgnoreCase)
                            .Trim());
                        item.SubItems.Add(evt.Arguments != null
                            ? string.Join(",", evt.Arguments.Where(x => !x.Value.GetType().IsArray).Select(x => x.Name + "=" + x.ToDisplayValue()))
                            : null);

                        ListView.Items.Add(item);
                    }
                }
                finally
                {
                    ListView.EndUpdate();
                }
            });
        }

        private void UpdateHighlight()
        {
            foreach (var item in ListView.Items.ToEnumerable<ListViewItem>())
            {
                var process = item.SubItems[2].Tag as TrackedProcessInvocation;

                item.ForeColor = HighlightedProcess != null && process == HighlightedProcess
                    ? HighlightedProcessForeColor
                    : HighlightedProcess != null && process.Topic == HighlightedProcess.Topic
                        ? HighlightedTopicForeColor
                        : ListView.ForeColor;

                item.BackColor = HighlightedProcess != null && process.Topic == HighlightedProcess.Topic
                    ? HighlightedTopicBackColor
                    : ListView.BackColor;
            }
        }
    }
}