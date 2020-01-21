#pragma warning disable CA2213 // Disposable fields should be disposed
namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    public partial class MainForm : Form
    {
        private readonly TabControl _sessionTabs;
        private readonly DiagnosticsStateManager _stateManager;
        private readonly Dictionary<string, SessionContainerManager> _sessionTabManagers = new Dictionary<string, SessionContainerManager>();

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

        private void SessionCreated(Session session)
        {
            Invoke((Action)delegate
            {
                var sessionContainer = new TabPage(session.SessionId)
                {
                    BorderStyle = BorderStyle.None,
                };

                var manager = new SessionContainerManager(_stateManager, session, sessionContainer);
                _sessionTabManagers.Add(session.SessionId, manager);
                _sessionTabs.TabPages.Add(sessionContainer);
                _sessionTabs.SelectedTab = sessionContainer;
            });
        }

        protected override void OnLoad(EventArgs e)
        {
            base.OnLoad(e);

            //this.MaximizeOnSecondaryScreen();
        }
    }
}
#pragma warning restore CA2213 // Disposable fields should be disposed