# Spreadsheet test fixtures

The xlsx and xlsm workbooks the tests need are built in memory with
`rust_xlsxwriter` (see `src/test_support.rs`). The files here are the ones that
library cannot produce.

| File | Contents | How it was made |
|---|---|---|
| `in.ods` | Sheets `Data` and `Other`. `Data` has a header row (`Name`, `Amount`, `Date`, `Double`, `Ok`), five item rows with dates from 2024-01-01 and the formulas `=B2*2` to `=B6*2` in column D, and `=SUM(B2:B6)` in B8. `Other!B1` is `=Data!B5*10`. Rows 12 to 15 hold a small table, and the sheet also carries a chart and an image. | Written by `rust_xlsxwriter` 0.99.1 as an xlsx, then converted by LibreOffice 26.8.0.3: `soffice --headless --convert-to ods in.xlsx`. |
| `in.xls` | The same workbook as `in.ods`. | The same xlsx, converted by LibreOffice 26.8.0.3: `soffice --headless --convert-to xls in.xlsx`. |
| `libreoffice.xlsx` | The same workbook as `in.ods`, as LibreOffice writes an xlsx: explicit `t="n"` and `s="0"` on cells, empty `<row>` elements down to row 20, a `calcPr` without `fullCalcOnLoad`, and no `calcChain.xml`. Used to check that patching keeps every part LibreOffice wrote. | The same xlsx, converted by LibreOffice 26.8.0.3 with `soffice --headless --convert-to xlsx`; its `docProps/app.xml` names that application. |
| `encrypted.xlsx` | A one-sheet xlsx (`Secret!A1` = 42) encrypted with the password `dbflux`. | A minimal xlsx package written with Python's `zipfile`, then encrypted with msoffcrypto-tool 6.0.0: `msoffcrypto-tool -e -p dbflux plain.xlsx encrypted.xlsx`. |
| `vbaProject.bin` | A VBA project, used only to make `rust_xlsxwriter` write an xlsm. | Copied from the `examples/` directory of `rust_xlsxwriter` 0.99.1 (MIT OR Apache-2.0). |
