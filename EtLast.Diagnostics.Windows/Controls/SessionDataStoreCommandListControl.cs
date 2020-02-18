namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionDataStoreCommandListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public DiagSession Session { get; }
        public ListView ListView { get; }

        public SessionDataStoreCommandListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, DiagSession session)
        {
            Container = container;
            Session = session;

            ListView = new ListView()
            {
                View = View.Details,
                Dock = DockStyle.Fill,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = false,
                GridLines = false,
                AllowColumnReorder = false,
                FullRowSelect = true,
                BorderStyle = BorderStyle.FixedSingle,
            };

            ListView.Columns.Add("timestamp", 85);
            ListView.Columns.Add("context", 200);
            ListView.Columns.Add("topic", 300);
            ListView.Columns.Add("process", 300);
            ListView.Columns.Add("kind", 60);
            ListView.Columns.Add("type", 300);
            ListView.Columns.Add("transaction", 85);
            ListView.Columns.Add("kind", 85);
            ListView.Columns.Add("location", 100);
            ListView.Columns.Add("command", 700);
            ListView.Columns.Add("arguments", 200);

            diagnosticsStateManager.OnDiagContextCreated += ec =>
            {
                if (ec.Session == session)
                {
                    ec.WholePlaybook.OnEventsAdded += OnEventsAdded;
                }
            };
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            var eventsQuery = abstractEvents.OfType<DataStoreCommandEvent>();

            var events = eventsQuery.ToList();
            if (events.Count == 0)
                return;

            ListView.BeginUpdate();
            try
            {
                foreach (var evt in events)
                {
                    var item = ListView.Items.Add(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture), -1);
                    item.SubItems.Add(playbook.DiagContext.Name);

                    var process = playbook.DiagContext.WholePlaybook.ProcessList[evt.ProcessInvocationUID];

                    item.SubItems.Add(process.Topic);
                    item.SubItems.Add(process.Name);
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
                }
            }
            finally
            {
                ListView.EndUpdate();
            }
        }
    }
}