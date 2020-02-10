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
        public ExecutionContext Context { get; }
        public Playbook CurrentPlaybook { get; private set; }
        public Control Container { get; }
        private readonly ListView _processList;
        private readonly ListView _counterList;
        private readonly ListView _dataStoreCommandList;
        private readonly Panel _timelineContainer;
        private readonly TrackBar _timelineTrackbar;
        private readonly System.Threading.Timer _timelineTrackbarTimer;
        private readonly System.Threading.Timer _processStatUpdaterTimer;
        private readonly Label _firstEventLabel;
        private readonly Label _lastEventLabel;
        private readonly Label _currentEventLabel;
        private readonly ToolTip _toolTip;

        public ExecutionContextContainerManager(ExecutionContext context, Control container)
        {
            Context = context;
            Container = container;
            Container.SuspendLayout();

            try
            {
                _toolTip = new ToolTip()
                {
                    ShowAlways = true,
                    AutoPopDelay = 5000,
                    InitialDelay = 0,
                    ReshowDelay = 500,
                    IsBalloon = true,
                };

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

                _processStatUpdaterTimer = new System.Threading.Timer((state) => UpdateProcessStats());
                _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);

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
                    Width = 1200,
                    BorderStyle = BorderStyle.FixedSingle,
                };

                _processList.Columns.Add("topic", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
                _processList.Columns.Add("process name", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
                _processList.Columns.Add("type", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;

                _processList.Columns.Add("invocation", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Center;
                _processList.Columns.Add("INPUT", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.Columns.Add("CREATE", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.Columns.Add("store", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.Columns.Add("DROP", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.Columns.Add("STAY", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.Columns.Add("OUT", (_processList.Width - SystemInformation.VerticalScrollBarWidth - 4) / 3 * 1 / 7).TextAlign = HorizontalAlignment.Right;
                _processList.ShowItemToolTips = true;
                _processList.MouseMove += ProcessList_MouseMove;
                //_processList.MouseLeave += (s, a) => _toolTip.SetToolTip(s as Control, null);

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
                    BorderStyle = BorderStyle.FixedSingle,
                };

                _counterList.Columns.Add("counter name", _counterList.Width - 150 - SystemInformation.VerticalScrollBarWidth - 4);
                _counterList.Columns.Add("value", 75);
                _counterList.Columns.Add("actual", 75);

                _dataStoreCommandList = new ListView()
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
                    BorderStyle = BorderStyle.FixedSingle,
                };

                _dataStoreCommandList.Columns.Add("timestamp", 85);
                _dataStoreCommandList.Columns.Add("topic", 300);
                _dataStoreCommandList.Columns.Add("process name", 300);
                _dataStoreCommandList.Columns.Add("process type", 300);
                _dataStoreCommandList.Columns.Add("text", 700);
                _dataStoreCommandList.Columns.Add("arguments", 200);

                context.WholePlaybook.OnProcessInvoked += OnProcessInvoked;
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

        private void ProcessList_MouseMove(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var item = list.GetItemAt(e.X, e.Y);
            var info = list.HitTest(e.X, e.Y);

            if (item != null && info.SubItem?.Tag is string text)
            {
                _toolTip.Show(text, list, e.X, e.Y);
            }
            else if (item != null && info.SubItem?.Tag is Func<string> textFunc)
            {
                _toolTip.Show(textFunc.Invoke(), list, e.X, e.Y);
            }
            else
            {
                _toolTip.SetToolTip(list, null);
            }
        }

        private void UpdateProcessStats()
        {
            _processStatUpdaterTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            _processList.Invoke(new Action(() =>
            {
                var changed = false;
                foreach (ListViewItem item in _processList.Items)
                {
                    if (item.Tag is TrackedProcessInvocation p)
                    {
                        if (item.SubItems[4].Text != p.InputRowCount.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[5].Text != p.CreatedRowCount.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[6].Text != p.StoredRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[7].Text != p.DroppedRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[8].Text != p.AliveRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[9].Text != p.PassedRowCount.ToString("D", CultureInfo.InvariantCulture))
                        {
                            changed = true;
                            break;
                        }
                    }
                }

                if (changed)
                {
                    _processList.BeginUpdate();
                    try
                    {
                        foreach (ListViewItem item in _processList.Items)
                        {
                            if (item.Tag is TrackedProcessInvocation p)
                            {
                                item.SubItems[4].SetIfChanged(p.InputRowCount.ToString("D", CultureInfo.InvariantCulture),
                                    () => string.Join("\n", p.InputRowCountByByPreviousProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToString("D", CultureInfo.InvariantCulture))));
                                item.SubItems[5].SetIfChanged(p.CreatedRowCount.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[6].SetIfChanged(p.StoredRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[7].SetIfChanged(p.DroppedRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[8].SetIfChanged(p.AliveRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[9].SetIfChanged(p.PassedRowCount.ToString("D", CultureInfo.InvariantCulture),
                                    () => string.Join("\n", p.PassedRowCountByNextProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToString("D", CultureInfo.InvariantCulture))));
                            }
                        }
                    }
                    finally
                    {
                        _processList.EndUpdate();
                    }
                }
            }));

            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            ProcessNewDataStoreCommands(abstractEvents);

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

        private void ProcessNewDataStoreCommands(List<AbstractEvent> abstractEvents)
        {
            var eventsQuery = abstractEvents.OfType<DataStoreCommandEvent>();
            /*if (ProcessUidFilter != null)
                eventsQuery = eventsQuery.Where(x => x.ProcessUid == ProcessUidFilter.Value);*/

            var events = eventsQuery.ToList();
            if (events.Count == 0)
                return;

            _dataStoreCommandList.Invoke((Action)delegate
            {
                _dataStoreCommandList.BeginUpdate();
                try
                {
                    foreach (var evt in events)
                    {
                        var item = _dataStoreCommandList.Items.Add(new DateTime(evt.Timestamp).ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture), -1);

                        var process = Context.WholePlaybook.ProcessList[evt.ProcessInvocationUID];

                        item.SubItems.Add(process.Topic);
                        item.SubItems.Add(process.Name);
                        item.SubItems.Add(process.Type);
                        item.SubItems.Add(evt.Command);
                        item.SubItems.Add(evt.Arguments != null
                            ? string.Join(",", evt.Arguments.Where(x => !x.Value.GetType().IsArray).Select(x => x.Name + "=" + x.ToDisplayValue()))
                            : null);
                    }
                }
                finally
                {
                    _dataStoreCommandList.EndUpdate();
                }
            });
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
            //_counterList.Invoke(new Action(() =>
            {
                UpdateCounters();
            }//));
        }

        private void UpdateCounters()
        {
            var counters = Context.WholePlaybook.Counters.Values.OrderBy(x => x.Name);

            _counterList.BeginUpdate();
            try
            {
                foreach (var actualCounter in counters)
                {
                    var item = _counterList.Items[actualCounter.Name];
                    if (item == null)
                    {
                        item = _counterList.Items.Add(actualCounter.Name, actualCounter.Name, -1);
                        item.SubItems.Add("-");
                        item.SubItems.Add("-");
                    }

                    Counter currentCounter = null;
                    CurrentPlaybook?.Counters?.TryGetValue(actualCounter.Name, out currentCounter);
                    item.SubItems[1].SetIfChanged(currentCounter?.ValueToString ?? "-");
                    item.SubItems[2].SetIfChanged(actualCounter.ValueToString);
                }
            }
            finally
            {
                _counterList.EndUpdate();
            }
        }

        private void OnProcessInvoked(Playbook playbook, TrackedProcessInvocation process)
        {
            //_processList.Invoke(new Action(() =>
            {
                var caller = process.CallerInvocationUID != null ? Context.WholePlaybook.ProcessList[process.CallerInvocationUID.Value] : null;
                var item = _processList.Items.Add(process.Topic);
                item.SubItems.Add((caller != null ? caller.Name + " -> " : null) + process.Name);
                item.SubItems.Add(process.Type);
                item.SubItems.Add(process.InstanceUID.ToString("D", CultureInfo.InvariantCulture)
                    + (process.InvocationCounter > 1
                        ? "/" + process.InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                        : ""));
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.Tag = process;

                Container_Resize(null, EventArgs.Empty);

                if (_processList.SelectedItems.Count == 0)
                {
                    item.Selected = true;
                }
            }//));
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

            _processList.Bounds = new Rectangle(0, _timelineContainer.Bounds.Height, _processList.Bounds.Width, (Container.Height - _timelineContainer.Bounds.Height) / 2);
            _counterList.Bounds = new Rectangle(0, _processList.Bounds.Bottom, _counterList.Bounds.Width, Container.Height - _processList.Bounds.Bottom);
            _dataStoreCommandList.Bounds = new Rectangle(_counterList.Bounds.Right, _counterList.Bounds.Top, Container.Width - _counterList.Bounds.Right, _counterList.Bounds.Height);

            _firstEventLabel.Location = new Point(0, 0);
            _lastEventLabel.Location = new Point(_timelineContainer.Width - _lastEventLabel.Width, 0);
        }
    }
}