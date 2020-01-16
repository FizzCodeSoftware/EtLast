namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
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

        public LogManager(DiagnosticsStateManager stateManager, Control container, Session session)
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
            };

            _output.AppendText("[SESSION STARTED] [" + Session.SessionId + "]" + Environment.NewLine);

            stateManager.OnNewEventArrived += NewEventArrived;
        }

        private void NewEventArrived(SessionContext context, AbstractEvent abstractEvent)
        {
            if (Session != null && context.Session != Session)
                return;

            switch (abstractEvent)
            {
                case RowCreatedEvent evt:
                    _output.Invoke((Action)delegate
                    {
                        _output.AppendText(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-CREATED] ");

                        if (evt.ProcessUid != null)
                        {
                            _output.AppendText("<" + evt.ProcessName + "> ");
                        }

                        _output.AppendText("UID=" + evt.RowUid.ToString("D", CultureInfo.InvariantCulture));

                        if (evt.Values != null)
                        {
                            _output.AppendText(", " + string.Join(", ", evt.Values.Select(x => x.Name + "=" + x.ToDisplayValue() + (x.Value == null ? "" : " (" + x.Value.GetType().Name + ")"))));
                        }

                        _output.AppendText(Environment.NewLine);
                    });
                    break;
                case RowValueChangedEvent evt:
                    _output.Invoke((Action)delegate
                    {
                        _output.AppendText(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-VALUE-CHANGED] ");

                        if (evt.ProcessUid != null)
                        {
                            _output.AppendText("<" + evt.ProcessName + "> ");
                        }

                        if (evt.OperationType != null)
                        {
                            _output.AppendText("(" + evt.OperationName + "/#" + evt.OperationNumber + ") ");
                        }

                        _output.AppendText("UID=" + evt.RowUid.ToString("D", CultureInfo.InvariantCulture)
                            + ", column: " + evt.Column + ", previous value: " + evt.PreviousValue.ToDisplayValue() + ", current value: " + evt.CurrentValue.ToDisplayValue());

                        _output.AppendText(Environment.NewLine);
                    });
                    break;
                case RowStoredEvent evt:
                    _output.Invoke((Action)delegate
                    {
                        _output.AppendText(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-STORED] UID=" + evt.RowUid.ToString("D", CultureInfo.InvariantCulture) + ", location: " + string.Join(" / ", evt.Locations.Select(x => x.Key + "=" + x.Value)));
                        _output.AppendText(Environment.NewLine);
                    });
                    break;
                case LogEvent evt:
                    _output.Invoke((Action)delegate
                    {
                        _output.AppendText(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [" + evt.Severity.ToShortString() + "] ");

                        if (evt.ProcessUid != null)
                        {
                            _output.AppendText("<" + evt.ProcessName + "> ");
                        }

                        if (evt.OperationType != null)
                        {
                            _output.AppendText("(" + evt.OperationName + "/#" + evt.OperationNumber + ") ");
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
                    });
                    break;
                case RowOwnerChangedEvent evt:
                    _output.Invoke((Action)delegate
                    {
                        _output.AppendText(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture) + " [" + context.FullName + "] [ROW-OWNER-CHANGED] ");

                        if (evt.NewProcessUid != null)
                        {
                            _output.AppendText("<" + evt.NewProcessName + "> ");
                        }

                        _output.AppendText("UID=" + evt.RowUid.ToString("D", CultureInfo.InvariantCulture));

                        _output.AppendText(Environment.NewLine);
                    });
                    break;
            }
        }
    }
}
