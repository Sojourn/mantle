#!/usr/bin/env python3

import argparse
import re
import sys

from collections import deque
from pathlib import Path
from typing import Union


SOURCE_EXTENSIONS = {'.c', '.cpp'}
HEADER_EXTENSIONS = {'.h', '.hpp'}

PRAGMA_ONCE_PATTERN = r'^#\s*pragma\s*once\s*$'
EXTERNAL_INCLUDE_PATTERN = r'^#\s*include\s*<(.+)>\s*$'
INTERNAL_INCLUDE_PATTERN = r'^#\s*include\s*"(.+)"\s*$'


def collect_file_paths(directory: Path, recursive: bool) -> list[Path]:
    """Returns a list of source and header file paths contained in this directory, and optionally sub-directories."""

    file_paths: list[Path] = []

    for item in directory.iterdir():
        if item.is_file() and (item.suffix in HEADER_EXTENSIONS or item.suffix in SOURCE_EXTENSIONS):
            file_paths.append(item)
        elif item.is_dir() and recursive:
            file_paths += collect_file_paths(item, recursive)

    return file_paths 


def read_file_lines(path: Path) -> list[str]:
    lines: list[str] = []

    with open(str(path), 'r+') as f:
        text = f.read(-1)
        for line in text.split('\n'):
            lines.append(line)

    return lines


class InputFile:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_name = file_path.name
        self.is_source_file = file_path.suffix in SOURCE_EXTENSIONS
        self.is_header_file = file_path.suffix in HEADER_EXTENSIONS

        # Remove the include path segment from header to get something like this:
        #    'include/mantle/foo.h' -> 'mantle/foo.h'
        self.include_file_path = None if self.is_source_file else Path(*file_path.parts[1:])

        # Read and process the contents of the file and scan for internal include macros.
        self.lines: list[str] = []
        self.external_includes: list[Path] = []
        self.internal_includes: list[Path] = []
        for line in read_file_lines(file_path):
            self.lines.append(line)
            self.external_includes += [Path(match) for match in re.findall(EXTERNAL_INCLUDE_PATTERN, line)]
            self.internal_includes += [Path(match) for match in re.findall(INTERNAL_INCLUDE_PATTERN, line)]

    def remove_pragma_once_lines(self):
        self.lines = [
            line for line in self.lines if not re.match(PRAGMA_ONCE_PATTERN, line)
        ]

    def remove_includes(self):
        self.remove_external_includes()
        self.remove_internal_includes()

    def remove_external_includes(self):
        self.lines = [
            line for line in self.lines if not re.match(EXTERNAL_INCLUDE_PATTERN, line)
        ]

    def remove_internal_includes(self):
        self.lines = [
            line for line in self.lines if not re.match(INTERNAL_INCLUDE_PATTERN, line)
        ]


class InputFileGraph:
    def __init__(self):
        self.nodes: list[InputFile] = []
        self.edges: list[tuple[InputFile, Path]] = [] # (source, target) pairs
        self.node_index: dict[Path, InputFile] = {} # include_file_path -> 

    def add_input_file(self, input_file: Union[Path, InputFile]) -> None:
        if isinstance(input_file, Path):
            input_file = InputFile(input_file)

        assert(input_file not in self.nodes)
        self.nodes.append(input_file)

        for internal_include in input_file.internal_includes:
            self.edges.append((input_file, internal_include))

    def topological_sort(self) -> list[InputFile]:
        # Build an index of `include_target -> InputFile`.
        header_file_index = {
            node.include_file_path: node
            for node in self.nodes if node.is_header_file
        }

        # Calculate in-degrees of include targets.
        in_degree = {node.include_file_path: 0 for node in self.nodes if node.is_header_file}
        for input_file, include_target in self.edges:
            in_degree[Path(include_target)] += 1

        # Queue for nodes with no incoming edges (source files or orphaned header files).
        queue = deque()
        for node in self.nodes:
            if node.is_source_file or (node.is_header_file and in_degree[node.include_file_path] == 0):
                queue.append(node)

        result: list[InputFile] = []
        while queue:
            node = queue.popleft()
            result.append(node)

            # Decrease the in-degree of each neighbor.
            for include_target in node.internal_includes:
                in_degree[Path(include_target)] -= 1
                if in_degree[Path(include_target)] == 0:
                    queue.append(header_file_index[Path(include_target)])

        if len(result) < len(self.nodes):
            raise RuntimeError('Input file graph contains a cycle')

        return list(reversed(result))


class OutputBuffer:
    def __init__(self):
        self._indent = 0
        self.lines: list[str] = []

    def indent(self) -> None:
        self._indent += 1

    def dedent(self) -> None:
        if self._indent > 0:
            self._indent -= 1

    def write_line(self, line: str) -> None:
        self.lines.append(f'{"    " * self._indent}{line}')

    def write_empty_line(self) -> None:
        self.lines.append('')

    def write_comment(self, comment: str) -> None:
        self.write_line(f'// {comment}')

    def write_pragma_once(self) -> None:
        self.write_line('#pragma once')

    def write_external_include(self, file_path: Path) -> None:
        self.write_line(f'#include <{file_path}>')


def main():
    parser = argparse.ArgumentParser(description='Create a single header library')
    parser.add_argument('--output', type=Path, required=True, help='File path of the resulting single-header library')
    parser.add_argument('--header-dir', type=Path, help='Directory containing header files')
    parser.add_argument('--source-dir', type=Path, help='Directory containing source files')
    parser.add_argument('--recursive', action='store_true', help='Recursively scan folders for header and source files')

    args = parser.parse_args()

    file_paths: list[str] = []
    if args.header_dir is not None:
        if args.header_dir.exists():
            file_paths += collect_file_paths(args.header_dir, args.recursive)
        else:
            print(f'Header directory does not exist - {args.header_dir}')
            sys.exit(1)
    if args.source_dir is not None:
        if args.source_dir.exists():
            file_paths += collect_file_paths(args.source_dir, args.recursive)
        else:
            print(f'Source directory does not exist - {args.source_dir}')
            sys.exit(1)

    if len(file_paths) == 0:
        print('No files found')
        sys.exit(1)

    input_file_graph = InputFileGraph()
    for file_path in file_paths:
        input_file_graph.add_input_file(InputFile(file_path))

    input_files = input_file_graph.topological_sort()

    output = OutputBuffer()
    output.write_pragma_once()
    output.write_empty_line()

    # Write a block of external include macros, sorted by file path.
    external_includes: list[Path] = []
    for input_file in input_files:
        external_includes += input_file.external_includes
    for external_include in sorted(list(set(external_includes))):
        output.write_external_include(external_include)
    output.write_empty_line()

    # Write input files.
    for input_file in input_files:
        input_file.remove_pragma_once_lines()
        input_file.remove_external_includes()
        input_file.remove_internal_includes()

        output.write_comment(f'{input_file.file_path}')
        for line in input_file.lines:
            if 'MANTLE_SOURCE_INLINE' in line:
                line = 'inline'
            output.write_line(line)
        output.write_empty_line()

    print('\n'.join(output.lines))


if __name__ == '__main__':
    main()
