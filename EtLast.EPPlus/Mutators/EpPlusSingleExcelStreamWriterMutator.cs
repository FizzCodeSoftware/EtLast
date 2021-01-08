namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.IO;
    using FizzCode.EtLast;
    using OfficeOpenXml;

    public class EpPlusSingleExcelStreamWriterMutator<TState> : AbstractMutator, IRowWriter
        where TState : BaseExcelWriterState, new()
    {
        public string StoreLocation { get; set; }
        public Stream Stream { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        private TState _state;
        private ExcelPackage _package;
        private int? _storeUid;

        public EpPlusSingleExcelStreamWriterMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _state = new TState();
        }

        protected override void CloseMutator()
        {
            if (_state.LastWorksheet != null)
            {
                Finalize?.Invoke(_package, _state);
            }

            if (ExistingPackage == null && _package != null)
            {
                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.streamWrite, Stream.GetType().GetFriendlyTypeName(), null, null, null, null,
                    "saving excel package to stream: {StreamType}",
                    Stream.GetType().GetFriendlyTypeName());

                try
                {
                    _package.Save();
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.streamWrite, iocUid, null);
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.streamWrite, iocUid, null, ex);
                    throw;
                }

                _package.Dispose();
                _package = null;
            }

            _state = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_package == null) // lazy load here instead of prepare
            {
                _package = ExistingPackage ?? new ExcelPackage(Stream);
                Initialize?.Invoke(_package, _state);
            }

            try
            {
                Action.Invoke(row, _package, _state);

                if (_storeUid != null)
                {
                    Context.RegisterRowStored(this, row, _storeUid.Value);
                }
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, row, "error raised during writing an excel stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel stream, message: {0}, row: {1}",
                    ex.Message, row.ToDebugString()));

                exception.Data.Add("FileName", Stream);
                throw exception;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (Stream == null)
                throw new ProcessParameterNullException(this, nameof(Stream));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));

            if (string.IsNullOrEmpty(StoreLocation))
                throw new ProcessParameterNullException(this, nameof(StoreLocation));
        }

        public void AddWorkSheet(string name)
        {
            _state.LastWorksheet = _package.Workbook.Worksheets.Add(name);
            _storeUid = Context.GetStoreUid(StoreLocation, name);
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusSingleExcelStreamWriterMutatorFluent
    {
        public static IFluentProcessMutatorBuilder WriteRowToExcelStreamCustom<TState>(this IFluentProcessMutatorBuilder builder, EpPlusSingleExcelStreamWriterMutator<TState> mutator)
        where TState : BaseExcelWriterState, new()
        {
            return builder.AddMutators(mutator);
        }
    }
}