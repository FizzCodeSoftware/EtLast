namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextCounterListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public ListView ListView { get; }

        private readonly System.Threading.Timer _statUpdateTimer;

        public ContextCounterListControl(Control container, AbstractDiagContext context)
        {
            Context = context;

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = false,
                GridLines = false,
                AllowColumnReorder = false,
                FullRowSelect = true,
                Width = 550,
                BorderStyle = BorderStyle.FixedSingle,
            };

            ListView.Columns.Add("value", 75);
            ListView.Columns.Add("counter name", 1000);

            _statUpdateTimer = new System.Threading.Timer((state) => UpdateStats());
            _statUpdateTimer.Change(500, System.Threading.Timeout.Infinite);
        }

        private void UpdateStats()
        {
            _statUpdateTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            ListView.Invoke(new Action(() =>
            {
                var foundInList = new HashSet<string>();

                var changed = false;
                foreach (ListViewItem item in ListView.Items)
                {
                    if (item.Tag is Counter counter)
                    {
                        foundInList.Add(counter.Name);
                        if (item.SubItems[0].Text != counter.ValueToString)
                            changed = true;
                    }
                }

                if (foundInList.Count != Context.WholePlaybook.Counters.Count)
                    changed = true;

                if (changed)
                {
                    ListView.BeginUpdate();
                    try
                    {
                        var missingCounters = Context.WholePlaybook.Counters.Values.Where(x => !foundInList.Contains(x.Name));
                        foreach (var counter in missingCounters)
                        {
                            var item = ListView.Items.Add(counter.ValueToString);
                            item.Tag = counter;
                            item.SubItems.Add(counter.Name);
                        }

                        foreach (ListViewItem item in ListView.Items)
                        {
                            if (item.Tag is Counter counter)
                            {
                                item.SubItems[0].SetIfChanged(counter.ValueToString);
                            }
                        }
                    }
                    finally
                    {
                        ListView.EndUpdate();
                    }
                }
            }));

            if (!Context.FullyLoaded)
            {
                _statUpdateTimer.Change(500, System.Threading.Timeout.Infinite);
            }
        }
    }
}