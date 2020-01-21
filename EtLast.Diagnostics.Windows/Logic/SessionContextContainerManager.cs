namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Drawing;
    using System.Linq;
    using System.Threading;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionContextContainerManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public SessionContext Context { get; }
        public Playbook CurrentPlaybook { get; private set; }
        public Control Container { get; }
        private readonly ListView _processList;
        private readonly ListView _counterList;
        private readonly Panel _timelineContainer;
        private readonly System.Threading.Timer _timelineTrackbarTimer;

        public SessionContextContainerManager(SessionContext context, Control container)
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
                };

                var timelineTrackbar = new TrackBar()
                {
                    Dock = DockStyle.Fill,
                    Minimum = 0,
                    Maximum = 100,
                };

                _timelineTrackbarTimer = new System.Threading.Timer((state) =>
                {
                    container.Invoke(new Action(() =>
                    {
                        timelineTrackbar.Enabled = false;
                        Snapshot(timelineTrackbar.Value);
                        timelineTrackbar.Enabled = true;
                    }));
                });

                timelineTrackbar.ValueChanged += (s, e) => _timelineTrackbarTimer.Change(500, Timeout.Infinite);
                _timelineContainer.Controls.Add(timelineTrackbar);

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

                _processList.Columns.Add("name", "process name", (_processList.Width - SystemInformation.VerticalScrollBarWidth) / 2);
                _processList.Columns.Add("type", "type", (_processList.Width - SystemInformation.VerticalScrollBarWidth) / 2);

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

                _counterList.Columns.Add("name", "counter name", _counterList.Width - 150 - SystemInformation.VerticalScrollBarWidth);
                _counterList.Columns.Add("current", "value", 75);
                _counterList.Columns.Add("actual", "actual", 75);

                context.WholePlaybook.OnProcessAdded += OnProcessAdded;
                context.WholePlaybook.OnCountersUpdated += OnCurrentCountersUpdated;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
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
            _counterList.Items.Clear();
            var counters = Context.WholePlaybook.Counters.OrderBy(x => x.Name);
            foreach (var actualCounter in counters)
            {
                var item = _counterList.Items.Add(actualCounter.Name);

                var currentCounter = CurrentPlaybook?.Counters?.Find(x => x.Code == actualCounter.Code);
                if (currentCounter != null)
                {
                    item.SubItems.Add(currentCounter.ValueToString);
                }
                else
                {
                    item.SubItems.Add("-");
                }

                item.SubItems.Add(actualCounter.ValueToString);
                item.Tag = actualCounter;
            }

            _counterList.AutoResizeColumns(ColumnHeaderAutoResizeStyle.ColumnContent);
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

        private void Snapshot(int percentage)
        {
            CurrentPlaybook = new Playbook(Context);
            for (var i = 0; i < Context.WholePlaybook.Events.Count / 100 * percentage; i++)
            {
                CurrentPlaybook.AddEvent(Context.WholePlaybook.Events[i]);
            }

            UpdateCounters();
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _timelineContainer.Bounds = new Rectangle(0, 0, Container.Width, 30);

            _processList.Bounds = new Rectangle(0, _timelineContainer.Height, _processList.Width, (Container.Height - _timelineContainer.Height) / 2);
            _counterList.Bounds = new Rectangle(0, _processList.Bottom, _counterList.Width, Container.Height - _processList.Bottom);
        }
    }
}