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
                    BorderStyle = BorderStyle.None,
                };

                _logManager = new LogManager(_stateManager, logContainer, Session);

                _contextTabs = new TabControl()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    Appearance = TabAppearance.FlatButtons,
                };
                _contextTabs.SelectedIndexChanged += SelectedContextTabChanged;

                Session.OnSessionContextCreated += OnSessionContextCreated;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void SelectedContextTabChanged(object sender, EventArgs e)
        {
            if (_contextTabs.SelectedIndex < 0)
                return;

            var context = _contextTabs.TabPages[_contextTabs.SelectedIndex].Tag as SessionContext;
            var contextManager = _contextContainerManagers[context.Name];
            contextManager.FocusProcessList();
        }

        private void OnSessionContextCreated(SessionContext context)
        {
            if (_contextContainerManagers.ContainsKey(context.Name))
                return;

            _contextTabs.Invoke(new Action(() =>
            {
                var contextContainer = new TabPage(context.Name)
                {
                    BorderStyle = BorderStyle.None,
                    Tag = context,
                };

                var contextManager = new SessionContextContainerManager(context, contextContainer);
                _contextContainerManagers.Add(context.Name, contextManager);

                _contextTabs.TabPages.Add(contextContainer);

                contextManager.FocusProcessList();
            }));
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _logManager.Container.Bounds = new System.Drawing.Rectangle(0, Container.Height - (Container.Bounds.Height / 4), Container.Width, Container.Bounds.Height / 4);
            _contextTabs.Bounds = new System.Drawing.Rectangle(0, 0, Container.Width, _logManager.Container.Top);
        }
    }
}