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
    internal class DataStoreCommandManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public Session Session { get; }
        private readonly ListView _list;

        public ExecutionContext ExecutionContextFilter { get; set; }
        public int? ProcessUidFilter { get; set; }

        public DataStoreCommandManager(Control container, Session session)
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
                BackColor = Color.Black,
                ForeColor = Color.LightGray,
                BorderStyle = BorderStyle.FixedSingle,
            };

            _list.Columns.Add("timestamp", 85);
            _list.Columns.Add("context", 200);
            _list.Columns.Add("topic", 250);
            _list.Columns.Add("process name", 250);
            _list.Columns.Add("process type", 250);
            _list.Columns.Add("operation type", 250);
            _list.Columns.Add("operation name", 150);
            _list.Columns.Add("text", 700);
            _list.Columns.Add("arguments", 200);

            session.OnExecutionContextCreated += ec =>
            {
                ec.WholePlaybook.OnEventsAdded += OnEventsAdded;
            };
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            if (ExecutionContextFilter != null && playbook?.ExecutionContext != ExecutionContextFilter)
                return;

            var eventsQuery = abstractEvents.OfType<DataStoreCommandEvent>();
            if (ProcessUidFilter != null)
                eventsQuery = eventsQuery.Where(x => x.ProcessUid == ProcessUidFilter.Value);

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

                        TrackedProcess process = null;
                        if (evt.ProcessUid != null)
                            playbook.ProcessList.TryGetValue(evt.ProcessUid.Value, out process);

                        item.SubItems.Add(process?.Topic);
                        item.SubItems.Add(process?.Name);
                        item.SubItems.Add(process?.Type);
                        item.SubItems.Add(evt.Operation?.Type);
                        item.SubItems.Add(evt.Operation?.InstanceName);
                        item.SubItems.Add(evt.Command);
                        item.SubItems.Add(evt.Arguments != null
                            ? string.Join(",", evt.Arguments.Select(x => x.Name + "=" + x.ToDisplayValue()))
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