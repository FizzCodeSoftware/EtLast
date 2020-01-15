#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    public partial class MainForm : Form
    {
        private readonly TabControl _sessionTabs;
        private readonly DiagnosticsStateManager _stateManager;
        private readonly Dictionary<string, SessionTabManager> _sessionTabManagers = new Dictionary<string, SessionTabManager>();

        public MainForm()
        {
            InitializeComponent();

            _stateManager = new DiagnosticsStateManager("http://+:8642/");
            _stateManager.OnSessionCreated += SessionCreated;

            _sessionTabs = new TabControl()
            {
                Dock = DockStyle.Fill,
                Parent = this,
            };

            _stateManager.Start();
        }

        private void SessionCreated(DiagnosticsSession session)
        {
            Invoke((Action)delegate
            {
                var sessionContainer = new TabPage(session.SessionId);
                var manager = new SessionTabManager(_stateManager, session, sessionContainer);
                _sessionTabManagers.Add(session.SessionId, manager);
                _sessionTabs.Controls.Add(sessionContainer);
                _sessionTabs.SelectedTab = sessionContainer;
            });
        }

        protected override void OnLoad(EventArgs e)
        {
            base.OnLoad(e);

            this.MaximizeOnSecondaryScreen();
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed