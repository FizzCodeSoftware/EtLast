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
        public Session Session { get; }
        private readonly ListView _list;

        public ExecutionContext ExecutionContextFilter { get; set; }
        public int? ProcessUidFilter { get; set; }

        public SessionDataStoreCommandListControl(Control container, DiagnosticsStateManager diagnosticsStateManager, Session session)
        {
            Container = container;
            Session = session;

            _list = new ListView()
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

            _list.Columns.Add("timestamp", 85);
            _list.Columns.Add("context", 200);
            _list.Columns.Add("topic", 300);
            _list.Columns.Add("process name", 300);
            _list.Columns.Add("process type", 300);
            _list.Columns.Add("text", 700);
            _list.Columns.Add("arguments", 200);

            diagnosticsStateManager.OnExecutionContextCreated += ec =>
            {
                if (ec.Session == session)
                {
                    ec.WholePlaybook.OnEventsAdded += OnEventsAdded;
                }
            };
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            if (ExecutionContextFilter != null && playbook?.ExecutionContext != ExecutionContextFilter)
                return;

            var eventsQuery = abstractEvents.OfType<DataStoreCommandEvent>();
            if (ProcessUidFilter != null)
                eventsQuery = eventsQuery.Where(x => x.ProcessInvocationUID == ProcessUidFilter.Value);

            var events = eventsQuery.ToList();
            if (events.Count == 0)
                return;

            _list.Invoke((Action)delegate
            {
                _list.BeginUpdate();
                try
                {
                    foreach (var evt in events)
                    {
                        var item = _list.Items.Add(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture), -1);
                        item.SubItems.Add(playbook.ExecutionContext.Name);

                        var process = playbook.ExecutionContext.WholePlaybook.ProcessList[evt.ProcessInvocationUID];

                        item.SubItems.Add(process.Topic);
                        item.SubItems.Add(process.Name);
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
                    }
                }
                finally
                {
                    _list.EndUpdate();
                }
            });
        }
    }
}