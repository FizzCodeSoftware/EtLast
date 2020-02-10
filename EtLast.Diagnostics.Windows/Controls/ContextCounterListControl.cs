namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextCounterListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public ExecutionContext Context { get; }
        public ListView CounterList { get; }

        private Playbook _currentPlaybook;

        public Playbook CurrentPlaybook
        {
            get => _currentPlaybook;
            set
            {
                _currentPlaybook = value;
                UpdateCounters();
            }
        }

        public ContextCounterListControl(Control container, ExecutionContext context)
        {
            Context = context;

            CounterList = new ListView()
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

            CounterList.Columns.Add("value", 75);
            CounterList.Columns.Add("actual", 75);
            CounterList.Columns.Add("counter name", CounterList.Width - 150 - SystemInformation.VerticalScrollBarWidth - 4);

            context.WholePlaybook.OnCountersUpdated += _ => UpdateCounters();
        }

        internal void UpdateCounters()
        {
            var counters = Context.WholePlaybook.Counters.Values.OrderBy(x => x.Name);

            CounterList.BeginUpdate();
            try
            {
                foreach (var actualCounter in counters)
                {
                    var item = CounterList.Items[actualCounter.Name];
                    if (item == null)
                    {
                        item = CounterList.Items.Add(actualCounter.Name, "-", -1);
                        item.SubItems.Add("-");
                        item.SubItems.Add(actualCounter.Name);
                    }

                    Counter currentCounter = null;
                    CurrentPlaybook?.Counters?.TryGetValue(actualCounter.Name, out currentCounter);
                    item.SubItems[0].SetIfChanged(currentCounter?.ValueToString ?? "-");
                    item.SubItems[1].SetIfChanged(actualCounter.ValueToString);
                }
            }
            finally
            {
                CounterList.EndUpdate();
            }
        }
    }
}