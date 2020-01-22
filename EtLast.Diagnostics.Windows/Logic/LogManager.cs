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

            session.OnExecutionContextCreated += executionContext => executionContext.WholePlaybook.OnEventsAdded += OnEventsAdded;
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            var logEvents = abstractEvents.OfType<LogEvent>().ToList();
            if (logEvents.Count == 0)
                return;

            _output.Invoke((Action)delegate
            {
                foreach (var evt in logEvents)
                {
                    _output.AppendText(new DateTime(evt.Ts).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + playbook.ExecutionContext.Name + "] [" + evt.Severity.ToShortString() + "] ");

                    if (evt.ProcessUid != null && playbook.ProcessList.TryGetValue(evt.ProcessUid.Value, out var process))
                    {
                        _output.AppendText("<" + process.Name + "> ");
                    }

                    if (evt.Operation != null)
                    {
                        _output.AppendText("(" + evt.Operation.Name + "/#" + evt.Operation.Number + ") ");
                    }

                    var text = evt.Text;
                    if (evt.Arguments != null)
                    {
                        foreach (var arg in evt.Arguments)
                        {
                            text = text.Replace(arg.Name, arg.ToDisplayValue(), StringComparison.InvariantCultureIgnoreCase);
                        }
                    }

                    _output.AppendText(text);
                    _output.AppendText(Environment.NewLine);
                }
            });
        }
    }
}