namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionTabManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public DiagnosticsSession Session { get; }
        public Control Container { get; }

        private readonly DiagnosticsStateManager _stateManager;
        private readonly TabControl _processTabs;
        private readonly LogManager _logManager;
        private readonly Playbook _currentPlaybook;

        public SessionTabManager(DiagnosticsStateManager stateManager, DiagnosticsSession session, Control container)
        {
            Session = session;
            _stateManager = stateManager;
            Container = container;
            Container.SuspendLayout();
            try
            {
                _processTabs = new TabControl()
                {
                    Dock = DockStyle.Fill,
                    Parent = container,
                };

                var logContainer = new Panel()
                {
                    Parent = container,
                    Dock = DockStyle.Bottom,
                };

                _logManager = new LogManager(_stateManager, logContainer);

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);

            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _logManager.Container.Height = Container.Bounds.Height / 4;
        }

        private void Snapshot()
        {
            for (var i = _processTabs.TabPages.Count - 1; i >= 0; i--)
            {
                _processTabs.TabPages.RemoveAt(i);
            }


        }
    }
}