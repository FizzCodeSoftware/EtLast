namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public DiagSession Session { get; }
        public Control Container { get; }

        private readonly TabControl _tabs;
        private readonly Dictionary<string, ContextControl> _contextContainerManagers = new Dictionary<string, ContextControl>();

        public SessionControl(DiagSession session, Control container, DiagnosticsStateManager diagnosticsStateManager)
        {
            Session = session;
            Container = container;
            Container.SuspendLayout();
            try
            {
                _tabs = new TabControl()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    Appearance = TabAppearance.FlatButtons,
                };
                _tabs.SelectedIndexChanged += SelectedContextTabChanged;

                /*var logContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    BorderStyle = BorderStyle.None,
                };*/

                var logContainer = new TabPage("LOG")
                {
                    BorderStyle = BorderStyle.None,
                };
                _tabs.TabPages.Add(logContainer);

                var logManager = new LogListControl(logContainer, diagnosticsStateManager, Session);

                /*var dataStoreCommandContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Right | AnchorStyles.Bottom,
                    BorderStyle = BorderStyle.None,
                };*/

                var dataStoreCommandContainer = new TabPage("DATA STORE COMMANDS")
                {
                    BorderStyle = BorderStyle.None,
                };
                _tabs.TabPages.Add(dataStoreCommandContainer);

                var dataStoreCommandManager = new SessionDataStoreCommandListControl(dataStoreCommandContainer, diagnosticsStateManager, Session);

                diagnosticsStateManager.OnDiagContextCreated += ec =>
                {
                    if (ec.Session == session)
                    {
                        OnDiagContextCreated(ec);
                    }
                };

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
            if (_tabs.SelectedIndex < 0)
                return;

            if (_tabs.TabPages[_tabs.SelectedIndex].Tag is AbstractDiagContext context)
            {
                _contextContainerManagers[context.Name].FocusProcessList();
            }
        }

        private void OnDiagContextCreated(AbstractDiagContext diagContext)
        {
            if (_contextContainerManagers.ContainsKey(diagContext.Name))
                return;

            var contextContainer = new TabPage(diagContext.Name)
            {
                BorderStyle = BorderStyle.None,
                Tag = diagContext,
            };

            var contextManager = new ContextControl(diagContext, contextContainer);
            _contextContainerManagers.Add(diagContext.Name, contextManager);

            _tabs.TabPages.Add(contextContainer);

            contextManager.FocusProcessList();
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _tabs.Bounds = new Rectangle(0, 0, Container.Width, Container.Height);
        }
    }
}
