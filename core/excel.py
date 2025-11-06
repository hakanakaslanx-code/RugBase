"""Minimal XLSX writer used for exporting data without external dependencies."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List
from xml.sax.saxutils import escape
import zipfile


def _column_letter(index: int) -> str:
    """Convert a 1-based column index to its Excel column letter."""
    if index < 1:
        raise ValueError("Column index must be 1 or greater")

    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _format_cell(reference: str, value: object) -> str:
    """Render a single cell in the worksheet XML."""
    if value is None:
        return f'<c r="{reference}"/>'

    if isinstance(value, bool):
        return f'<c r="{reference}" t="b"><v>{1 if value else 0}</v></c>'

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{reference}"><v>{value}</v></c>'

    text = escape(str(value))
    return (
        f'<c r="{reference}" t="inlineStr"><is><t xml:space="preserve">{text}'
        "</t></is></c>"
    )


class Worksheet:
    """Represents a single worksheet within the minimal workbook."""

    def __init__(self) -> None:
        self.rows: List[List[object]] = []

    def append(self, values: Iterable[object]) -> None:
        self.rows.append(list(values))

    def render(self) -> str:
        row_fragments: List[str] = []
        for row_index, row in enumerate(self.rows, start=1):
            cell_fragments = []
            for column_index, value in enumerate(row, start=1):
                reference = f"{_column_letter(column_index)}{row_index}"
                cell_fragments.append(_format_cell(reference, value))
            cells = "".join(cell_fragments)
            row_fragments.append(f'<row r="{row_index}">{cells}</row>')

        sheet_data = "".join(row_fragments)
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            f"<sheetData>{sheet_data}</sheetData>"
            "</worksheet>"
        )


class Workbook:
    """A tiny XLSX workbook implementation tailored for RugBase exports."""

    def __init__(self) -> None:
        self.active = Worksheet()

    def save(self, filename: str) -> None:
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        core_properties = _CORE_PROPERTIES_TEMPLATE.format(timestamp=timestamp)
        worksheet_xml = self.active.render()

        with zipfile.ZipFile(filename, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("[Content_Types].xml", _CONTENT_TYPES_XML)
            archive.writestr("_rels/.rels", _PACKAGE_RELS_XML)
            archive.writestr("docProps/app.xml", _APP_PROPERTIES_XML)
            archive.writestr("docProps/core.xml", core_properties)
            archive.writestr("xl/workbook.xml", _WORKBOOK_XML)
            archive.writestr("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS_XML)
            archive.writestr("xl/worksheets/sheet1.xml", worksheet_xml)


_CONTENT_TYPES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""


_PACKAGE_RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
</Relationships>
"""


_APP_PROPERTIES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>RugBase</Application>
  <DocSecurity>0</DocSecurity>
  <ScaleCrop>false</ScaleCrop>
  <HeadingPairs>
    <vt:vector size="2" baseType="variant">
      <vt:variant>
        <vt:lpstr>Worksheets</vt:lpstr>
      </vt:variant>
      <vt:variant>
        <vt:i4>1</vt:i4>
      </vt:variant>
    </vt:vector>
  </HeadingPairs>
  <TitlesOfParts>
    <vt:vector size="1" baseType="lpstr">
      <vt:lpstr>Sheet1</vt:lpstr>
    </vt:vector>
  </TitlesOfParts>
</Properties>
"""


_CORE_PROPERTIES_TEMPLATE = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>RugBase</dc:creator>
  <cp:lastModifiedBy>RugBase</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:modified>
</cp:coreProperties>
"""


_WORKBOOK_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
"""


_WORKBOOK_RELS_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>
"""


__all__ = ["Workbook"]

