namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class LogManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public Session Session { get; }
        private readonly RichTextBox _output;

        public LogManager(Control container, Session session)
        {
            Container = container;
            Session = session;

            _output = new RichTextBox
            {
                Parent = container,
                Dock = DockStyle.Fill,
                ReadOnly = true,
                BackColor = Color.Black,
                ForeColor = Color.LightGray,
                Font = new Font("CONSOLAS", 11.0f),
                HideSelection = false,
                BorderStyle = BorderStyle.FixedSingle,
            };

            _output.AppendText("[SESSION STARTED] [" + Session.SessionId + "]" + Environment.NewLine);

            session.OnExecutionContextCreated += ec =>
            {
                ec.WholePlaybook.OnEventsAdded += OnEventsAdded;
            };
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            var events = abstractEvents.OfType<LogEvent>().ToList();
            if (events.Count == 0)
                return;

            _output.Invoke((Action)delegate
            {
                var sb = new StringBuilder();
                foreach (var evt in events)
                {
                    sb
                        .Append(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture))
                        .Append(" [")
                        .Append(playbook.ExecutionContext.Name)
                        .Append("] [")
                        .Append(evt.Severity.ToShortString())
                        .Append("] ");

                    if (evt.ProcessUid != null && playbook.ProcessList.TryGetValue(evt.ProcessUid.Value, out var process))
                    {
                        sb
                            .Append('<')
                            .Append(process.Name)
                            .Append("> ");
                    }

                    if (evt.Operation != null)
                    {
                        sb
                            .Append('(')
                            .Append(evt.Operation.ToDisplayValue())
                            .Append(") ");
                    }

                    var text = evt.Text;
                    if (evt.Arguments != null)
                    {
                        foreach (var arg in evt.Arguments)
                        {
                            text = text.Replace(arg.Name, arg.ToDisplayValue(), StringComparison.InvariantCultureIgnoreCase);
                        }
                    }

                    sb.AppendLine(text);
                }

                _output.AppendText(sb.ToString());
            });
        }
    }
}