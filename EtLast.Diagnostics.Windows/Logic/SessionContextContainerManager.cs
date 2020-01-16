namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
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
                timelineTrackbar.ValueChanged += (s, e) => _timelineTrackbarTimer.Change(TimeSpan.FromMilliseconds(500), new TimeSpan(-1));
                _timelineContainer.Controls.Add(timelineTrackbar);

                _processList = new ListView()
                {
                    View = View.List,
                    Parent = container,
                };

                context.WholePlaybook.OnProcessAdded += OnProcessAdded;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        internal void OnProcessAdded(Playbook playbook, TrackedProcess process)
        {
            _processList.Invoke(new Action(() =>
            {
                var item = _processList.Items.Add(process.Name);
                item.Tag = process;
            }));
        }

        private void Snapshot(int percentage)
        {
            CurrentPlaybook = new Playbook(Context);
            for (var i = 0; i < Context.WholePlaybook.Events.Count / 100 * percentage; i++)
            {
                CurrentPlaybook.AddEvent(Context.WholePlaybook.Events[i]);
            }
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _timelineContainer.Bounds = new System.Drawing.Rectangle(0, 0, Container.Width, 30);
            _processList.Bounds = new System.Drawing.Rectangle(0, _timelineContainer.Height, 200, Container.Height - _timelineContainer.Height);
        }
    }
}