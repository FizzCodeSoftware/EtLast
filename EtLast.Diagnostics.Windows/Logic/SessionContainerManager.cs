namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionContainerManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Session Session { get; }
        public Control Container { get; }

        private readonly DiagnosticsStateManager _stateManager;
        private readonly TabControl _contextTabs;
        private readonly LogManager _logManager;
        private readonly Dictionary<string, SessionContextContainerManager> _contextContainerManagers = new Dictionary<string, SessionContextContainerManager>();

        public SessionContainerManager(DiagnosticsStateManager stateManager, Session session, Control container)
        {
            Session = session;
            _stateManager = stateManager;
            Container = container;
            Container.SuspendLayout();
            try
            {
                var logContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                };

                _logManager = new LogManager(_stateManager, logContainer, Session);

                _contextTabs = new TabControl()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                };

                Session.OnSessionContextCreated += OnSessionContextCreated;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void OnSessionContextCreated(SessionContext context)
        {
            context.WholePlaybook.OnProcessAdded += OnProcessAdded;
        }

        private void OnProcessAdded(Playbook playbook, TrackedProcess process)
        {
            if (_contextContainerManagers.ContainsKey(playbook.Context.FullName))
                return;

            _contextTabs.Invoke(new Action(() =>
            {
                var contextContainer = new TabPage(playbook.Context.Name);

                var contextManager = new SessionContextContainerManager(playbook.Context, contextContainer);
                _contextContainerManagers.Add(playbook.Context.FullName, contextManager);

                _contextTabs.TabPages.Add(contextContainer);

                contextManager.OnProcessAdded(playbook, process);
            }));
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _logManager.Container.Bounds = new System.Drawing.Rectangle(0, Container.Height - (Container.Bounds.Height / 4), Container.Width, Container.Bounds.Height / 4);
            _contextTabs.Bounds = new System.Drawing.Rectangle(0, 0, Container.Width, _logManager.Container.Top);
        }
    }
}