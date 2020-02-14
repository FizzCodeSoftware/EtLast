namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Globalization;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public ExecutionContext Context { get; }
        public Playbook CurrentPlaybook { get; private set; }
        public Control Container { get; }
        private readonly Panel _timelineContainer;
        private readonly TrackBar _timelineTrackbar;
        private readonly System.Threading.Timer _timelineTrackbarTimer;
        private readonly Label _firstEventLabel;
        private readonly Label _lastEventLabel;
        private readonly Label _currentEventLabel;
        public ContextProcessInvocationListControl ProcessInvocationList { get; }
        public ContextCounterListControl CounterList { get; }
        public ContextDataStoreCommandListControl DataStoreCommandList { get; }

        public ContextControl(ExecutionContext context, Control container)
        {
            Context = context;
            Container = container;
            Container.SuspendLayout();

            try
            {
                _timelineContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Top | AnchorStyles.Right,
                    BorderStyle = BorderStyle.None,
                    Enabled = false,
                };

                _firstEventLabel = new Label()
                {
                    Visible = false,
                    AutoSize = true,
                    BackColor = Color.Transparent,
                };

                _lastEventLabel = new Label()
                {
                    Visible = false,
                    AutoSize = true,
                    BackColor = Color.Transparent,
                };

                _currentEventLabel = new Label()
                {
                    Visible = false,
                    AutoSize = true,
                    BackColor = Color.Transparent,
                };

                _timelineTrackbar = new TrackBar()
                {
                    Dock = DockStyle.Fill,
                    Minimum = 0,
                    Maximum = 0,
                    TickFrequency = 1000000,
                };

                _timelineTrackbarTimer = new System.Threading.Timer((state) =>
                {
                    container.Invoke(new Action(() =>
                    {
                        _timelineTrackbar.Enabled = false;
                        Snapshot(_timelineTrackbar.Value);
                        _timelineTrackbar.Enabled = true;
                    }));
                });

                _timelineTrackbar.ValueChanged += (s, e) => _timelineTrackbarTimer.Change(500, System.Threading.Timeout.Infinite);
                _timelineContainer.Controls.Add(_timelineTrackbar);
                _timelineContainer.Controls.Add(_firstEventLabel);
                _timelineContainer.Controls.Add(_lastEventLabel);
                _timelineContainer.Controls.Add(_currentEventLabel);

                ProcessInvocationList = new ContextProcessInvocationListControl(container, context);
                CounterList = new ContextCounterListControl(container, context);
                DataStoreCommandList = new ContextDataStoreCommandListControl(container, context)
                {
                    LinkedProcessInvocationList = ProcessInvocationList
                };

                context.WholePlaybook.OnEventsAdded += OnEventsAdded;

                ProcessInvocationList.OnSelectionChanged += ProcessInvocationList_OnSelectionChanged;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void ProcessInvocationList_OnSelectionChanged(TrackedProcessInvocation process)
        {
            DataStoreCommandList.HighlightedProcess = process;
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            DataStoreCommandList.ProcessNewDataStoreCommands(abstractEvents);

            if (playbook.FirstEventTimestamp == null || playbook.LastEventTimestamp == null)
                return;

            if (playbook.FirstEventTimestamp.Value == playbook.LastEventTimestamp.Value)
                return;

            if (!_timelineContainer.Enabled)
            {
                _timelineContainer.Enabled = true;
                _firstEventLabel.Text = playbook.FirstEventTimestamp.Value.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

                _firstEventLabel.Visible = true;
                _lastEventLabel.Visible = true;

                _firstEventLabel.BringToFront();
                _lastEventLabel.BringToFront();
                _currentEventLabel.BringToFront();

                _firstEventLabel.Location = new Point(0, 0);
            }

            _lastEventLabel.Text = playbook.LastEventTimestamp.Value.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
            _lastEventLabel.Location = new Point(_timelineContainer.Width - _lastEventLabel.Width, 0);

            _timelineTrackbar.Maximum = Convert.ToInt32(playbook.LastEventTimestamp.Value.Subtract(playbook.FirstEventTimestamp.Value).TotalMilliseconds);
            UpdateCurrentEventLabelPosition();
        }

        private void UpdateCurrentEventLabelPosition()
        {
            _currentEventLabel.Location = new Point(Convert.ToInt32(((double)_timelineContainer.Width * _timelineTrackbar.Value / _timelineTrackbar.Maximum) - (_currentEventLabel.Width / 2)), 0);
        }

        internal void FocusProcessList()
        {
            if (ProcessInvocationList.ListView.SelectedItems.Count == 0 && ProcessInvocationList.ListView.Items.Count > 0)
            {
                ProcessInvocationList.ListView.Items[0].Selected = true;
            }

            ProcessInvocationList.ListView.Focus();
        }

        private void Snapshot(int msecDiff)
        {
            CurrentPlaybook = new Playbook(Context);

            var selectedTime = Context.WholePlaybook.FirstEventTimestamp.Value.AddMilliseconds(msecDiff);

            var events = Context.GetEventsUntil(selectedTime);
            CurrentPlaybook.AddEvents(events);

            _currentEventLabel.Text = CurrentPlaybook.LastEventTimestamp.Value.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
            if (!_currentEventLabel.Visible)
            {
                _currentEventLabel.Visible = true;
                _currentEventLabel.BringToFront();
            }

            UpdateCurrentEventLabelPosition();

            CounterList.CurrentPlaybook = CurrentPlaybook;
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _timelineContainer.Bounds = new Rectangle(0, 0, Container.Width, 30);

            ProcessInvocationList.ListView.Bounds = new Rectangle(0, _timelineContainer.Bounds.Height, Container.Width - CounterList.CounterList.Width, (Container.Height - _timelineContainer.Bounds.Height) / 2);

            CounterList.CounterList.Bounds = new Rectangle(Container.Width - CounterList.CounterList.Width, ProcessInvocationList.ListView.Bounds.Top, CounterList.CounterList.Width, ProcessInvocationList.ListView.Bounds.Height);
            DataStoreCommandList.ListView.Bounds = new Rectangle(0, ProcessInvocationList.ListView.Bounds.Bottom, Container.Width, Container.Height - ProcessInvocationList.ListView.Bounds.Bottom);

            _firstEventLabel.Location = new Point(0, 0);
            _lastEventLabel.Location = new Point(_timelineContainer.Width - _lastEventLabel.Width, 0);
        }
    }
}