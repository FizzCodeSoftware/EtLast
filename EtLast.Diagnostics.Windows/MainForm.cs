#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    public partial class MainForm : Form
    {
        private readonly TabControl _sessionTabs;
        private readonly DiagnosticsStateManager _stateManager;
        private readonly Dictionary<string, SessionContainerManager> _sessionTabManagers = new Dictionary<string, SessionContainerManager>();
        private readonly Timer _timer;

        public MainForm()
        {
            InitializeComponent();

            Font = new Font(Font.FontFamily, 9, FontStyle.Regular);

            _stateManager = new DiagnosticsStateManager("http://+:8642/");
            _stateManager.OnSessionCreated += SessionCreated;

            _sessionTabs = new TabControl()
            {
                Dock = DockStyle.Fill,
                Parent = this,
            };

            _timer = new Timer()
            {
                Interval = 500,
                Enabled = false,
            };
            _timer.Tick += UpdateTimerTick;

            _stateManager.Start();
            _timer.Start();
        }

        private void UpdateTimerTick(object sender, EventArgs e)
        {
            _timer.Stop();
            _stateManager.ProcessEvents();
            _timer.Start();
        }

        private void SessionCreated(Session session)
        {
            Invoke((Action)delegate
            {
                var sessionContainer = new TabPage(session.SessionId)
                {
                    BorderStyle = BorderStyle.None,
                };

                var manager = new SessionContainerManager(session, sessionContainer);
                _sessionTabManagers.Add(session.SessionId, manager);
                _sessionTabs.TabPages.Add(sessionContainer);
                _sessionTabs.SelectedTab = sessionContainer;
            });
        }

        protected override void OnLoad(EventArgs e)
        {
            base.OnLoad(e);

            MaximizeOnSecondaryScreen();
        }

        private void MaximizeOnSecondaryScreen()
        {
            var screen = Screen.AllScreens.Length == 1 || true
                ? Screen.PrimaryScreen
                : Screen.AllScreens
                    .Where(x => !x.Primary)
                    .OrderByDescending(x => x.Bounds.Width)
                    .First();

            SuspendLayout();
            Bounds = new Rectangle(screen.Bounds.Left, screen.Bounds.Top, Bounds.Width, Bounds.Height);
            WindowState = FormWindowState.Maximized;
            ResumeLayout();
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed