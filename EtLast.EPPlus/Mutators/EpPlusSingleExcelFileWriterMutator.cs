namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.IO;
    using OfficeOpenXml;

    public sealed class EpPlusSingleExcelFileWriterMutator<TState> : AbstractMutator, IRowSink
        where TState : BaseExcelWriterState, new()
    {
        public string FileName { get; init; }
        public Action<ExcelPackage, TState> Initialize { get; init; }
        public Action<IRow, ExcelPackage, TState> Action { get; init; }
        public Action<ExcelPackage, TState> Finalize { get; init; }
        public ExcelPackage ExistingPackage { get; init; }

        private TState _state;
        private ExcelPackage _package;
        private int? _sinkUid;
        private int _rowCount;

        public EpPlusSingleExcelFileWriterMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override void StartMutator()
        {
            _state = new TState();
        }

        protected override void CloseMutator()
        {
            if (_state.Worksheet != null)
            {
                Finalize?.Invoke(_package, _state);
            }

            if (ExistingPackage == null && _package != null)
            {
                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.fileWrite, Path.GetDirectoryName(FileName), Path.GetFileName(FileName), null, null, null, null,
                    "saving excel package to {FileName}",
                    FileName);

                try
                {
                    _package.Save();
                    Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, iocUid, _rowCount);
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, iocUid, null, ex);
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
                _package = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
                Initialize?.Invoke(_package, _state);
            }

            try
            {
                Action.Invoke(row, _package, _state);

                if (_sinkUid != null)
                {
                    Context.RegisterWriteToSink(row, _sinkUid.Value);
                    _rowCount++;
                }
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}",
                    FileName, ex.Message, row.ToDebugString()));

                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }

        public void AddWorkSheet(string name)
        {
            _state.Worksheet = _package.Workbook.Worksheets.Add(name);
            _sinkUid = Context.GetSinkUid(FileName, name);
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EpPlusSingleExcelFileWriterMutatorFluent
    {
        public static IFluentProcessMutatorBuilder WriteRowToExcelFileCustom<TState>(this IFluentProcessMutatorBuilder builder, EpPlusSingleExcelFileWriterMutator<TState> mutator)
        where TState : BaseExcelWriterState, new()
        {
            return builder.AddMutator(mutator);
        }
    }
}