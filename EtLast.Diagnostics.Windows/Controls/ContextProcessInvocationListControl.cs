﻿namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    public delegate void OnProcessInvocationListSelectionChanged(TrackedProcessInvocation process);

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextProcessInvocationListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public ExecutionContext Context { get; }
        public ListView ListView { get; }
        public OnProcessInvocationListSelectionChanged OnSelectionChanged { get; set; }

        private readonly System.Threading.Timer _processStatUpdaterTimer;
        private readonly Dictionary<int, ListViewItem> _listViewItemsByProcessInvocationUID = new Dictionary<int, ListViewItem>();

        private Color IsOutputBackColor { get; set; } = Color.FromArgb(180, 255, 180);
        private Color IsInputBackColor { get; set; } = Color.FromArgb(255, 230, 185);
        private Color IsSameTopicBackColor { get; set; } = Color.FromArgb(220, 220, 255);

        public ContextProcessInvocationListControl(Control container, ExecutionContext context)
        {
            Context = context;

            _processStatUpdaterTimer = new System.Threading.Timer((state) => UpdateProcessStats());

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = false,
                GridLines = true,
                AllowColumnReorder = false,
                FullRowSelect = true,
                Width = 1200,
                BorderStyle = BorderStyle.FixedSingle,
            };

            var fix = 40 + 40 + 60 + 100;
            ListView.Columns.Add("#", 40);
            ListView.Columns.Add("time", 40);

            ListView.Columns.Add("topic", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("process", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("kind", 60).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("type", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;

            ListView.Columns.Add("IN", 100).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("+", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("-", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("store", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("pending", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("OUT", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.ShowItemToolTips = true;
            ListView.MouseMove += ListView_MouseMove;
            ListView.MouseLeave += (s, a) => ToolTipSingleton.Remove(s as Control);
            ListView.MultiSelect = false;
            ListView.HideSelection = false;

            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);

            context.WholePlaybook.OnProcessInvoked += OnProcessInvoked;

            ListView.ItemSelectionChanged += ListView_SelectedIndexChanged;
        }

        internal void SelectProcess(TrackedProcessInvocation process)
        {
            foreach (ListViewItem item in ListView.Items)
            {
                if (item.Tag == process)
                {
                    if (!item.Selected)
                    {
                        ListView.EnsureVisible(item.Index);

                        ListView.Focus();
                        item.Selected = true;
                    }
                }
            }
        }

        private void ListView_SelectedIndexChanged(object sender, ListViewItemSelectionChangedEventArgs e)
        {
            if (!e.IsSelected)
                return;

            var selectedProcess = e.Item.Tag as TrackedProcessInvocation;

            OnSelectionChanged?.Invoke(selectedProcess);

            foreach (var item in ListView.Items.ToEnumerable<ListViewItem>())
            {
                if (!(item.Tag is TrackedProcessInvocation itemProcess))
                    continue;

                if (selectedProcess != null)
                {
                    item.UseItemStyleForSubItems = false;

                    var itemIsInput = itemProcess.InputRowCountByByPreviousProcess.ContainsKey(selectedProcess.InvocationUID);
                    var itemIsOutput = selectedProcess.InputRowCountByByPreviousProcess.ContainsKey(itemProcess.InvocationUID);
                    var isSameTopic = selectedProcess.Topic == itemProcess.Topic/* || itemProcess.HasParentWithTopic(selectedProcess.Topic)*/;

                    item.SubItems[2].BackColor = isSameTopic
                        ? IsSameTopicBackColor
                        : item.BackColor;

                    if (itemIsInput)
                    {
                        item.SubItems[3].BackColor = item.SubItems[6].BackColor = IsInputBackColor;
                        item.SubItems[11].BackColor = ListView.BackColor;
                    }
                    else if (itemIsOutput)
                    {
                        item.SubItems[3].BackColor = item.SubItems[11].BackColor = IsOutputBackColor;
                        item.SubItems[6].BackColor = ListView.BackColor;
                    }
                    else if (isSameTopic)
                    {
                        item.SubItems[3].BackColor = IsSameTopicBackColor;
                        item.SubItems[6].BackColor = item.SubItems[11].BackColor = ListView.BackColor;
                    }
                    else
                    {
                        item.SubItems[3].BackColor = item.SubItems[11].BackColor = item.SubItems[6].BackColor = ListView.BackColor;
                    }

                    /*item.BackColor = itemIsOutput
                        ? IsOutputBackColor
                        : itemIsInput
                            ? IsInputBackColor
                            : isSameTopic
                                ? IsSameTopicBackColor
                                : ListView.BackColor;*/
                }
                else
                {
                    item.UseItemStyleForSubItems = true;
                }
            }
        }

        private void OnProcessInvoked(Playbook playbook, TrackedProcessInvocation process)
        {
            var item = new ListViewItem(process.InstanceUID.ToString("D", CultureInfo.InvariantCulture)
                + (process.InvocationCounter > 1
                    ? "/" + process.InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                    : ""))
            {
                Tag = process,
            };

            item.SubItems.Add("-");
            item.SubItems.Add(process.Topic);
            item.SubItems.Add(process.IdentedName);
            item.SubItems.Add(process.KindToString());
            item.SubItems.Add(process.ShortType);
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.Tag = process;

            if (ListView.SelectedItems.Count == 0)
            {
                item.Selected = true;
            }

            if (process.Invoker != null && _listViewItemsByProcessInvocationUID.TryGetValue(process.Invoker.InvocationUID, out var invokerItem))
            {
                var nextIndex = invokerItem.Index + 1;
                while (nextIndex < ListView.Items.Count)
                {
                    var p = ListView.Items[nextIndex].Tag as TrackedProcessInvocation;
                    if (!p.HasParent(process.Invoker))
                        break;

                    nextIndex++;
                }

                ListView.Items.Insert(nextIndex, item);
            }
            else
            {
                ListView.Items.Add(item);
            }

            _listViewItemsByProcessInvocationUID.Add(process.InvocationUID, item);
        }

        private void UpdateProcessStats()
        {
            _processStatUpdaterTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            ListView.Invoke(new Action(() =>
            {
                var changed = false;
                foreach (ListViewItem item in ListView.Items)
                {
                    if (item.Tag is TrackedProcessInvocation p)
                    {
                        if (item.SubItems[1].Text != p.ElapsedMillisecondsAfterFinishedAsString)
                        {
                            changed = true;
                            break;
                        }

                        if (item.SubItems[6].Text != p.GetFormattedInputRowCount()
                            || item.SubItems[7].Text != p.CreatedRowCount.ToStringNoZero()
                            || item.SubItems[8].Text != p.DroppedRowList.Count.ToStringNoZero()
                            || item.SubItems[9].Text != p.StoredRowList.Count.ToStringNoZero()
                            || item.SubItems[10].Text != p.AliveRowList.Count.ToStringNoZero()
                            || item.SubItems[11].Text != p.PassedRowCount.ToStringNoZero())
                        {
                            changed = true;
                            break;
                        }
                    }
                }

                if (changed)
                {
                    ListView.BeginUpdate();
                    try
                    {
                        foreach (ListViewItem item in ListView.Items)
                        {
                            if (item.Tag is TrackedProcessInvocation p)
                            {
                                item.SubItems[1].SetIfChanged(p.ElapsedMillisecondsAfterFinishedAsString);

                                item.SubItems[6].SetIfChanged(p.GetFormattedInputRowCount(),
                                    () => "INPUT:\n" + string.Join("\n", p.InputRowCountByByPreviousProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToStringNoZero())));
                                item.SubItems[7].SetIfChanged(p.CreatedRowCount.ToStringNoZero());
                                item.SubItems[8].SetIfChanged(p.DroppedRowList.Count.ToStringNoZero());
                                item.SubItems[9].SetIfChanged(p.StoredRowList.Count.ToStringNoZero());
                                item.SubItems[10].SetIfChanged(p.AliveRowList.Count.ToStringNoZero());
                                item.SubItems[11].SetIfChanged(p.PassedRowCount.ToStringNoZero(),
                                    () => string.Join("\n", p.PassedRowCountByNextProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToStringNoZero())));
                            }
                        }
                    }
                    finally
                    {
                        ListView.EndUpdate();
                    }
                }
            }));

            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);
        }

        private void ListView_MouseMove(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var info = list.HitTest(e.X, e.Y);

            if (info.SubItem?.Tag != null)
            {
                ToolTipSingleton.Show(info.SubItem.Tag, list, e.X, e.Y);
            }
            else
            {
                ToolTipSingleton.Remove(list);
            }
        }
    }
}