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
    internal class ExecutionContextContainerManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Interface.ExecutionContext Context { get; }
        public Playbook CurrentPlaybook { get; private set; }
        public Control Container { get; }
        private readonly ListView _processList;
        private readonly ListView _counterList;
        private readonly Panel _timelineContainer;
        private readonly TrackBar _timelineTrackbar;
        private readonly System.Threading.Timer _timelineTrackbarTimer;
        private readonly Label _firstEventLabel;
        private readonly Label _lastEventLabel;
        private readonly Label _currentEventLabel;

        public ExecutionContextContainerManager(ExecutionContext context, Control container)
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

                _processList = new ListView()
                {
                    View = View.Details,
                    Parent = container,
                    HeaderStyle = ColumnHeaderStyle.Nonclickable,
                    HideSelection = false,
                    GridLines = false,
                    AllowColumnReorder = false,
                    FullRowSelect = true,
                    BackColor = Color.Black,
                    ForeColor = Color.LightGray,
                    Width = 400,
                };

                _processList.Columns.Add("process name", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 2);
                _processList.Columns.Add("type", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 2);

                _counterList = new ListView()
                {
                    View = View.Details,
                    Parent = container,
                    HeaderStyle = ColumnHeaderStyle.Nonclickable,
                    HideSelection = false,
                    GridLines = false,
                    AllowColumnReorder = false,
                    FullRowSelect = true,
                    BackColor = Color.Black,
                    ForeColor = Color.LightGray,
                    Width = _processList.Width,
                };

                _counterList.Columns.Add("counter name", _counterList.Width - 150 - SystemInformation.VerticalScrollBarWidth - 4);
                _counterList.Columns.Add("value", 75);
                _counterList.Columns.Add("actual", 75);

                context.WholePlaybook.OnProcessAdded += OnProcessAdded;
                context.WholePlaybook.OnCountersUpdated += OnCurrentCountersUpdated;
                context.WholePlaybook.OnEventsAdded += OnEventsAdded;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> newEvents)
        {
            if (playbook.Events.Count < 3)
                return;

            _timelineContainer.Invoke(new Action(() =>
            {
                if (!_timelineContainer.Enabled)
                {
                    _timelineContainer.Enabled = true;
                    _firstEventLabel.Text = new DateTime(Context.WholePlaybook.Events[0].Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

                    _firstEventLabel.Visible = true;
                    _lastEventLabel.Visible = true;

                    _firstEventLabel.BringToFront();
                    _lastEventLabel.BringToFront();
                    _currentEventLabel.BringToFront();

                    _firstEventLabel.Location = new Point(0, 0);
                }

                var last = Context.WholePlaybook.Events[Context.WholePlaybook.Events.Count - 1];
                _lastEventLabel.Text = new DateTime(last.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
                _lastEventLabel.Location = new Point(_timelineContainer.Width - _lastEventLabel.Width, 0);

                _timelineTrackbar.Maximum = (int)(playbook.Events[playbook.Events.Count - 1].Timestamp - playbook.Events[0].Timestamp);
                UpdateCurrentEventLabelPosition();
            }));
        }

        private void UpdateCurrentEventLabelPosition()
        {
            _currentEventLabel.Location = new Point(Convert.ToInt32(((double)_timelineContainer.Width * _timelineTrackbar.Value / _timelineTrackbar.Maximum) - (_currentEventLabel.Width / 2)), 0);
        }

        internal void FocusProcessList()
        {
            if (_processList.SelectedItems.Count == 0 && _processList.Items.Count > 0)
            {
                _processList.Items[0].Selected = true;
            }

            _processList.Focus();
        }

        private void OnCurrentCountersUpdated(Playbook playbook)
        {
            _counterList.Invoke(new Action(() =>
            {
                UpdateCounters();
            }));
        }

        private void UpdateCounters()
        {
            var counters = Context.WholePlaybook.Counters.Values.OrderBy(x => x.Name);

            foreach (var actualCounter in counters)
            {
                var item = _counterList.Items[actualCounter.Name];

                if (!_counterList.Items.ContainsKey(actualCounter.Name))
                {
                    item = _counterList.Items.Add(actualCounter.Name, actualCounter.Name, -1);
                    item.SubItems.Add("-");
                    item.SubItems.Add("-");
                }
                else
                {
                    item = _counterList.Items[actualCounter.Name];
                }

                Counter currentCounter = null;
                CurrentPlaybook?.Counters?.TryGetValue(actualCounter.Name, out currentCounter);
                item.SubItems[1].Text = currentCounter?.ValueToString ?? "-";
                item.SubItems[2].Text = actualCounter.ValueToString;
            }
        }

        private void OnProcessAdded(Playbook playbook, TrackedProcess process)
        {
            _processList.Invoke(new Action(() =>
            {
                var item = _processList.Items.Add(process.Name);
                item.SubItems.Add(process.Type);
                item.Tag = process;
                // todo: add columns with process row statistics

                Container_Resize(null, EventArgs.Empty);

                if (_processList.SelectedItems.Count == 0)
                {
                    item.Selected = true;
                }
            }));
        }

        private void Snapshot(int tickDiff)
        {
            CurrentPlaybook = new Playbook(Context);

            var selectedTick = Context.WholePlaybook.Events[0].Timestamp + tickDiff;

            var events = new List<AbstractEvent>();
            var idx = 0;
            while (idx < Context.WholePlaybook.Events.Count)
            {
                var evt = Context.WholePlaybook.Events[idx++];
                if (evt.Timestamp > selectedTick)
                    break;

                events.Add(evt);
            }

            CurrentPlaybook.AddEvents(events);

            _currentEventLabel.Text = new DateTime(CurrentPlaybook.Events.Last().Timestamp).ToString("yyyy.MM.dd HH:mm:ss", CultureInfo.InvariantCulture);
            if (!_currentEventLabel.Visible)
            {
                _currentEventLabel.Visible = true;
                _currentEventLabel.BringToFront();
            }

            UpdateCurrentEventLabelPosition();

            UpdateCounters();
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _timelineContainer.Bounds = new Rectangle(0, 0, Container.Width, 30);

            _processList.Bounds = new Rectangle(0, _timelineContainer.Height, _processList.Width, (Container.Height - _timelineContainer.Height) / 2);
            _counterList.Bounds = new Rectangle(0, _processList.Bottom, _counterList.Width, Container.Height - _processList.Bottom);

            _firstEventLabel.Location = new Point(0, 0);
            _lastEventLabel.Location = new Point(_timelineContainer.Width - _lastEventLabel.Width, 0);
        }
    }
}