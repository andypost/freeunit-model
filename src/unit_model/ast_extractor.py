"""FreeUnit C Schema & AST Extractor using tree-sitter.

Parses FreeUnit C source headers to extract:
1. Enums and wire protocol message IDs.
2. Constants and preprocessor `#define` values.
3. Struct and union definitions with exact field types, bitfields, and byte offsets.
4. Layout validation against Linux x86_64 ABI.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

def _find_default_src() -> str:
    if "UNIT_SRC" in os.environ:
        return os.environ["UNIT_SRC"]
    root = Path(__file__).resolve().parents[2]
    candidates = [
        root.parent / "unit",
        root.parent / "freeunit",
        root / "src",
        Path.cwd() / "src",
        Path.cwd().parent / "unit",
    ]
    for cand in candidates:
        if (cand / "src" / "nxt_main.c").is_file():
            return str(cand)
        if (cand / "nxt_main.c").is_file():
            return str(cand.parent if cand.name == "src" else cand)
    return str(root)

DEFAULT_SRC = _find_default_src()

import tree_sitter as ts
import tree_sitter_c as tsc


# Macros to blank or rewrite during tree-sitter preprocessing
SHIM_BLANK = [
    "nxt_inline",
    "nxt_noinline",
    "nxt_cdecl",
    "nxt_aligned",
    "nxt_packed",
    "NXT_EXPORT",
    "nxt_fast_path",
    "nxt_slow_path",
    "nxt_fallthrough",
    "volatile",
    "__attribute__",
]

SHIM_LOOP = [
    ("nxt_queue_each", "nxt_queue_loop"),
    ("nxt_list_each", "nxt_list_loop"),
    ("nxt_http_fields_each", "nxt_http_fields_loop"),
    ("nxt_process_port_each", "nxt_process_port_loop"),
    ("nxt_runtime_process_each", "nxt_runtime_process_loop"),
]


def _balanced_end(text: str, i: int) -> int:
    depth = 0
    while i < len(text):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


def apply_shim(src: bytes) -> bytes:
    """Preprocess C code to neutralize macros that disrupt tree-sitter parsing."""
    text = src.decode("latin-1")
    buf = bytearray(src)

    def blank(s: int, e: int) -> None:
        for i in range(s, e):
            if buf[i] != 0x0A:
                buf[i] = 0x20

    # Blank static assertions: nxt_static_assert(...)
    for m in re.finditer(r"\bnxt_static_assert\s*\(", text):
        end = _balanced_end(text, m.end() - 1)
        blank(m.start(), end)

    # Blank attribute annotations: __attribute__((...))
    for m in re.finditer(r"\b__attribute__\s*\(", text):
        end = _balanced_end(text, m.end() - 1)
        blank(m.start(), end)

    # Rewrite loops
    for open_name, _ in SHIM_LOOP:
        for m in re.finditer(r"\b" + open_name + r"\s*\(", text):
            end = _balanced_end(text, m.end() - 1)
            stub = "for (;;)"[: end - m.start()]
            buf[m.start() : m.start() + len(stub)] = stub.encode()
            blank(m.start() + len(stub), end)

    # Blank keyword decorators
    for name in SHIM_BLANK + [c for _, c in SHIM_LOOP]:
        for m in re.finditer(r"\b" + re.escape(name) + r"\b", text):
            end = m.end()
            if end < len(text) and text[end] == "(":
                end = _balanced_end(text, end)
            blank(m.start(), end)

    return bytes(buf)


# Primitive type sizes and alignments on Linux x86_64
PRIMITIVE_SIZES: Dict[str, Tuple[int, int]] = {
    "char": (1, 1),
    "int8_t": (1, 1),
    "uint8_t": (1, 1),
    "u_char": (1, 1),
    "bool": (1, 1),
    "nxt_bool_t": (1, 1),
    "short": (2, 2),
    "int16_t": (2, 2),
    "uint16_t": (2, 2),
    "nxt_app_nncq_cycle_t": (2, 2),
    "int": (4, 4),
    "int32_t": (4, 4),
    "uint32_t": (4, 4),
    "nxt_int_t": (4, 4),
    "nxt_uint_t": (4, 4),
    "nxt_pid_t": (4, 4),
    "nxt_port_id_t": (2, 2),
    "nxt_chunk_id_t": (4, 4),
    "nxt_app_nncq_atomic_t": (4, 4),
    "nxt_atomic_t": (4, 4),
    "nxt_fd_t": (4, 4),
    "long": (8, 8),
    "int64_t": (8, 8),
    "uint64_t": (8, 8),
    "size_t": (8, 8),
    "ssize_t": (8, 8),
    "intptr_t": (8, 8),
    "uintptr_t": (8, 8),
    "nxt_atomic_uint_t": (8, 8),
    "nxt_free_map_t": (8, 8),
    "pointer": (8, 8),
}


@dataclass
class CField:
    name: str
    type_name: str
    size: int
    alignment: int
    offset: int
    is_pointer: bool = False
    is_array: bool = False
    array_len: int = 1
    is_bitfield: bool = False
    bit_width: int = 0
    bit_offset: int = 0


@dataclass
class CStruct:
    name: str
    is_union: bool = False
    size: int = 0
    alignment: int = 1
    fields: List[CField] = field(default_factory=list)


@dataclass
class CEnum:
    name: str
    values: Dict[str, int] = field(default_factory=dict)


class SchemaExtractor:
    """Extracts schema, constants, structs, and enums from FreeUnit C source files."""

    def __init__(self, src_root: Union[str, Path]):
        self.src_root = Path(src_root).resolve()
        self.parser = ts.Parser(ts.Language(tsc.language()))
        self.constants: Dict[str, int] = {}
        self.enums: Dict[str, CEnum] = {}
        self.structs: Dict[str, CStruct] = {}
        self.typedefs: Dict[str, str] = {}
        self.port_handlers: List[str] = []

    def load_all(self) -> None:
        """Parse all relevant C headers in order."""
        headers = [
            "src/nxt_types.h",
            "src/nxt_port_queue.h",
            "src/nxt_app_nncq.h",
            "src/nxt_app_queue.h",
            "src/nxt_port_memory_int.h",
            "src/nxt_port_memory.h",
            "src/nxt_port.h",
            "src/nxt_unit_typedefs.h",
            "src/nxt_unit_sptr.h",
            "src/nxt_unit_field.h",
            "src/nxt_unit_request.h",
            "src/nxt_unit_response.h",
            "src/nxt_unit_websocket.h",
        ]

        for rel in headers:
            path = self.src_root / rel
            if path.exists():
                self.parse_file(path)

        self._resolve_port_msg_constants()
        self._calculate_layouts()

    def parse_file(self, path: Path) -> None:
        """Parse a single C file with tree-sitter and regex macro scanning."""
        raw = path.read_bytes()
        self._extract_preproc_defines(raw)

        shimmed = apply_shim(raw)
        tree = self.parser.parse(shimmed)
        self._walk_node(tree.root_node, path.name)

    def _extract_preproc_defines(self, raw: bytes) -> None:
        """Extract constant #define values."""
        text = raw.decode("latin-1")
        for line in text.splitlines():
            line = line.strip()
            m = re.match(r"^#define\s+([A-Za-z0-9_]+)\s+([^/]+)", line)
            if m:
                name, val_expr = m.group(1), m.group(2).strip()
                if "(" in name:
                    continue  # Skip macro functions
                # Evaluate simple numeric expressions
                val = self._eval_expr(val_expr)
                if val is not None:
                    self.constants[name] = val

    def _eval_expr(self, expr: str) -> Optional[int]:
        """Safely evaluate arithmetic/constant expression."""
        expr = expr.strip()
        # Strip trailing comments or semicolons
        expr = re.sub(r"/\*.*?\*/", "", expr)
        expr = re.sub(r"//.*$", "", expr).strip()
        expr = expr.rstrip(";")
        # Strip U/UL/ULL suffixes
        expr = re.sub(r"([0-9]+)[uUlL]+", r"\1", expr)

        # Substitute known constants
        for k, v in sorted(self.constants.items(), key=lambda x: -len(x[0])):
            expr = re.sub(r"\b" + k + r"\b", str(v), expr)

        try:
            # Safe evaluation of integer expressions
            if re.match(r"^[0-9xXa-fA-F+\-*/% ()~^|&<>=!]+$", expr):
                return int(eval(expr, {"__builtins__": None}, {}))
        except Exception:
            pass
        return None

    def _walk_node(self, node: ts.Node, filename: str) -> None:
        """Recursively inspect tree-sitter AST nodes."""
        if node.type == "struct_specifier":
            self._handle_struct_specifier(node)
        elif node.type == "type_definition":
            self._handle_type_definition(node)
        elif node.type == "enum_specifier":
            self._handle_enum_specifier(node, None)

        for child in node.children:
            self._walk_node(child, filename)

    def _handle_struct_specifier(self, node: ts.Node) -> None:
        name_node = node.child_by_field_name("name")
        body_node = node.child_by_field_name("body")
        if not body_node or not name_node:
            return
        struct_name = name_node.text.decode()
        self._parse_struct_body(struct_name, body_node, is_union=False)

    def _handle_type_definition(self, node: ts.Node) -> None:
        decl = node.child_by_field_name("declarator")
        type_node = node.child_by_field_name("type")
        if not decl or not type_node:
            return
        typedef_name = decl.text.decode()

        if type_node.type == "enum_specifier":
            self._handle_enum_specifier(type_node, typedef_name)
        elif type_node.type in ("struct_specifier", "union_specifier"):
            body_node = type_node.child_by_field_name("body")
            is_union = type_node.type == "union_specifier"
            if body_node:
                self._parse_struct_body(typedef_name, body_node, is_union=is_union)
            else:
                inner_name = type_node.child_by_field_name("name")
                if inner_name:
                    self.typedefs[typedef_name] = inner_name.text.decode()
        else:
            self.typedefs[typedef_name] = type_node.text.decode()

    def _handle_enum_specifier(self, node: ts.Node, typedef_name: Optional[str]) -> None:
        body = node.child_by_field_name("body")
        if not body:
            return
        name_node = node.child_by_field_name("name")
        enum_name = typedef_name or (name_node.text.decode() if name_node else "anonymous")
        enum_obj = CEnum(name=enum_name)

        current_val = 0
        for child in body.children:
            if child.type == "enumerator":
                k_node = child.child_by_field_name("name")
                v_node = child.child_by_field_name("value")
                k_str = k_node.text.decode() if k_node else ""
                if not k_str:
                    continue

                if v_node:
                    val = self._eval_expr(v_node.text.decode())
                    if val is not None:
                        current_val = val
                    else:
                        # Try evaluating nxt_msg_last or nxt_port_handler_idx
                        raw_v = v_node.text.decode()
                        if "nxt_port_handler_idx(" in raw_v:
                            field_name = re.search(r"nxt_port_handler_idx\(([^)]+)\)", raw_v)
                            if field_name and self.port_handlers:
                                fname = field_name.group(1).strip()
                                if fname in self.port_handlers:
                                    current_val = self.port_handlers.index(fname)
                        elif "nxt_msg_last(" in raw_v:
                            inner = re.search(r"nxt_msg_last\(([^)]+)\)", raw_v)
                            if inner:
                                inner_val = self.constants.get(inner.group(1).strip())
                                if inner_val is not None:
                                    current_val = inner_val | 0x100

                enum_obj.values[k_str] = current_val
                self.constants[k_str] = current_val
                current_val += 1

        self.enums[enum_name] = enum_obj

    def _parse_struct_body(self, name: str, body: ts.Node, is_union: bool) -> None:
        """Extract fields and store raw CStruct."""
        cstruct = CStruct(name=name, is_union=is_union)
        for child in body.children:
            if child.type == "field_declaration":
                type_node = child.child_by_field_name("type")
                decl_node = child.child_by_field_name("declarator")
                if not type_node:
                    continue

                type_str = type_node.text.decode().strip()
                field_name = decl_node.text.decode().strip() if decl_node else ""
                is_pointer = "*" in field_name or "*" in type_str
                is_array = "[" in field_name
                array_len = 1
                is_bitfield = False
                bit_width = 0

                # Bitfield check: uint8_t skip:1;
                bitfield_match = re.search(r":\s*([0-9]+)", child.text.decode())
                if bitfield_match:
                    is_bitfield = True
                    bit_width = int(bitfield_match.group(1))
                    field_name = re.sub(r":\s*[0-9]+.*", "", field_name).strip()

                if is_array:
                    arr_match = re.search(r"\[([^\]]*)\]", field_name)
                    if arr_match:
                        dim_expr = arr_match.group(1).strip()
                        if dim_expr:
                            dim_val = self._eval_expr(dim_expr)
                            array_len = dim_val if dim_val is not None else 0
                        else:
                            array_len = 0  # Flexible array member: fields[]
                    field_name = re.sub(r"\[.*?\]", "", field_name).strip()

                field_name = field_name.replace("*", "").strip()

                # If special struct nxt_port_handlers_s, track handler field names
                if name == "nxt_port_handlers_s" and field_name:
                    self.port_handlers.append(field_name)

                fld = CField(
                    name=field_name,
                    type_name=type_str.replace("*", "").strip(),
                    size=0,
                    alignment=1,
                    offset=0,
                    is_pointer=is_pointer,
                    is_array=is_array,
                    array_len=array_len,
                    is_bitfield=is_bitfield,
                    bit_width=bit_width,
                )
                cstruct.fields.append(fld)

        self.structs[name] = cstruct

    def _resolve_port_msg_constants(self) -> None:
        """Resolve message type indices against port handlers."""
        for idx, hname in enumerate(self.port_handlers):
            msg_name = f"_NXT_PORT_MSG_{hname.upper()}"
            self.constants[msg_name] = idx
            self.constants[f"NXT_PORT_MSG_{hname.upper()}"] = idx
            self.constants[f"NXT_PORT_MSG_{hname.upper()}_LAST"] = idx | 0x100

        # Special overrides defined in nxt_port.h
        self.constants["NXT_PORT_MSG_LAST"] = 0x100
        self.constants["NXT_PORT_MSG_CLOSE_FD"] = 0x200
        self.constants["NXT_PORT_MSG_SYNC"] = 0x400
        self.constants["NXT_PORT_MSG_MASK"] = 0xFF
        self.constants["NXT_PORT_MSG_MMAP"] = (
            self.constants.get("NXT_PORT_MSG_MMAP_LAST", 16 | 0x100) | 0x400
        )

    def _resolve_type(self, type_name: str) -> Tuple[int, int]:
        """Return (size, alignment) of a resolved type."""
        # Check typedefs
        while type_name in self.typedefs:
            type_name = self.typedefs[type_name]

        if type_name in PRIMITIVE_SIZES:
            return PRIMITIVE_SIZES[type_name]

        if type_name in self.structs:
            s = self.structs[type_name]
            return s.size, s.alignment

        if type_name.startswith("struct "):
            sname = type_name.replace("struct ", "")
            if sname in self.structs:
                s = self.structs[sname]
                return s.size, s.alignment

        # Fallback default
        return 4, 4

    def _calculate_layouts(self) -> None:
        """Compute bit/byte offsets and struct sizes under Linux x86_64 ABI."""
        for sname, s in self.structs.items():
            if s.is_union:
                max_size = 0
                max_align = 1
                for fld in s.fields:
                    base_sz, base_al = (8, 8) if fld.is_pointer else self._resolve_type(fld.type_name)
                    sz = (base_sz * fld.array_len) if fld.is_array and fld.array_len > 0 else base_sz
                    fld.size = sz
                    fld.alignment = base_al
                    fld.offset = 0
                    max_size = max(max_size, sz)
                    max_align = max(max_align, base_al)
                s.size = ((max_size + max_align - 1) // max_align) * max_align
                s.alignment = max_align
            else:
                curr_offset = 0
                max_align = 1
                bit_pos = 0
                in_bitfield = False
                bitfield_base_sz = 0

                for fld in s.fields:
                    base_sz, base_al = (8, 8) if fld.is_pointer else self._resolve_type(fld.type_name)
                    if fld.is_bitfield:
                        if not in_bitfield:
                            # Start bitfield storage unit
                            if curr_offset % base_al != 0:
                                curr_offset += base_al - (curr_offset % base_al)
                            in_bitfield = True
                            bitfield_base_sz = base_sz
                            bit_pos = 0

                        if bit_pos + fld.bit_width > bitfield_base_sz * 8:
                            # Overflow to next unit
                            curr_offset += bitfield_base_sz
                            if curr_offset % base_al != 0:
                                curr_offset += base_al - (curr_offset % base_al)
                            bit_pos = 0

                        fld.offset = curr_offset
                        fld.bit_offset = bit_pos
                        fld.size = base_sz
                        fld.alignment = base_al
                        bit_pos += fld.bit_width
                        continue
                    else:
                        if in_bitfield:
                            # Finish bitfield storage unit
                            curr_offset += bitfield_base_sz
                            in_bitfield = False

                    fld.alignment = base_al
                    max_align = max(max_align, base_al)

                    # Align offset
                    if curr_offset % base_al != 0:
                        curr_offset += base_al - (curr_offset % base_al)

                    fld.offset = curr_offset
                    sz = (base_sz * fld.array_len) if fld.is_array and fld.array_len > 0 else base_sz
                    if fld.array_len == 0 and fld.is_array:
                        sz = 0  # Flexible array member
                    fld.size = sz
                    curr_offset += sz

                if in_bitfield:
                    curr_offset += bitfield_base_sz

                # Align struct total size to max_align
                if max_align > 0 and curr_offset % max_align != 0:
                    curr_offset += max_align - (curr_offset % max_align)
                s.size = curr_offset
                s.alignment = max_align



def print_summary(schema: SchemaExtractor) -> None:
    """Print extracted summary to stdout."""
    print("=" * 70)
    print("FreeUnit Schema Summary")
    print("=" * 70)
    print(f"Constants extracted: {len(schema.constants)}")
    print(f"Enums extracted:     {len(schema.enums)}")
    print(f"Structs extracted:   {len(schema.structs)}")
    print(f"Port handlers:       {len(schema.port_handlers)}")
    print("-" * 70)

    key_structs = [
        "nxt_port_msg_t",
        "nxt_port_mmap_msg_s",
        "nxt_app_queue_item_t",
        "nxt_unit_field_s",
        "nxt_unit_request_s",
        "nxt_unit_response_s",
    ]

    for name in key_structs:
        if name in schema.structs:
            s = schema.structs[name]
            print(f"\nStruct {s.name} (size: {s.size} bytes, align: {s.alignment}):")
            for fld in s.fields:
                bits = f" :{fld.bit_width}" if fld.is_bitfield else ""
                arr = f"[{fld.array_len}]" if fld.is_array else ""
                print(f"  +0x{fld.offset:02X} {fld.name}{arr}{bits}: {fld.type_name} ({fld.size}B)")


def main() -> int:
    parser = argparse.ArgumentParser(description="FreeUnit C AST Schema Extractor")
    parser.add_argument(
        "--src",
        default=DEFAULT_SRC,
        help="Path to FreeUnit C source directory (default: $UNIT_SRC, else <harness>/src)",
    )
    parser.add_argument("--summary", action="store_true", help="Print schema summary")
    args = parser.parse_args()

    extractor = SchemaExtractor(args.src)
    extractor.load_all()

    if args.summary:
        print_summary(extractor)
    else:
        print(f"Successfully extracted {len(extractor.structs)} structs, "
              f"{len(extractor.enums)} enums, and {len(extractor.constants)} constants.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
