import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import codecs
import csv
from parse_fixed_width import (
    read_spec,
    check_column_offset_match,
    parse_fixed_width,
    write_to_csv
)

# Test class for the read_spec function
class TestReadSpec(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open,
           read_data='{"ColumnNames": ["Name", "Age"], "Offsets": ["10", "3"], "FixedWidthEncoding": "utf-8", "IncludeHeader": "true", "DelimitedEncoding": "utf-8"}')
    def test_read_spec_valid(self, mock_open):

        # Test reading a valid specification file.

        result = read_spec('spec.json')
        self.assertEqual(result, (["Name", "Age"], [10, 3], "utf-8", "true", "utf-8"))

    @patch("builtins.open", new_callable=mock_open,
           read_data='{"ColumnNames": ["Name", "Age"], "Offsets": ["10", "3"], "FixedWidthEncoding": "invalid-encoding", "IncludeHeader": "true", "DelimitedEncoding": "utf-8"}')
    def test_read_spec_invalid_encoding(self, mock_open):

        # invalid encoding value.

        result = read_spec('spec.json')
        self.assertIsNone(result)

    @patch("builtins.open", new_callable=mock_open,
           read_data='{"ColumnNames": ["Name", "Age"], "Offsets": ["10", "3"], "FixedWidthEncoding": "utf-8", "IncludeHeader": "maybe", "DelimitedEncoding": "utf-8"}')
    def test_read_spec_invalid_include_header(self, mock_open):

        # invalid IncludeHeader value.

        result = read_spec('spec.json')
        self.assertIsNone(result)


# Test class for the check_column_offset_match function
class TestCheckColumnOffsetMatch(unittest.TestCase):

    def test_matching_columns_offsets(self):
        # check_matching_column_offset_match
        self.assertTrue(check_column_offset_match(["Name", "Age"], [10, 3]))

    def test_non_matching_columns_offsets(self):

        # check non matching column_offset
        self.assertFalse(check_column_offset_match(["Name"], [10, 3]))


# Test class for the parse_fixed_width function
class TestParseFixedWidth(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="John    25\nJane    30\n")
    def test_parse_fixed_width(self, mock_open):

        # Test parsing a fixed-width file.

        parsed_data = parse_fixed_width('data.txt', [8, 3], 'utf-8')
        self.assertEqual(parsed_data, [["John", "25"], ["Jane", "30"]])

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_parse_fixed_width_empty_file(self, mock_open):

        # Test parsing an empty fixed-width file.

        parsed_data = parse_fixed_width('data.txt', [10, 3], 'utf-8')
        self.assertEqual(parsed_data, [])

    @patch("builtins.open", new_callable=mock_open, read_data="John Doe  25\nJane Smith  30\n")
    def test_parse_fixed_width_malformed_data(self, mock_open):

        # Test parsing malformed fixed-width data.
        parsed_data = parse_fixed_width('data.txt', [8, 3], 'utf-8')
        self.assertEqual(parsed_data, [["John Doe", "2"], ["Jane Smi", "th"]])


# Test class for the write_to_csv function
class TestWriteToCSV(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("csv.writer")
    def test_write_to_csv(self, mock_csv_writer, mock_open):

        # Test writing data to a CSV file.

        mock_writer = MagicMock()
        mock_csv_writer.return_value = mock_writer

        # Call the function to test
        write_to_csv([["John", "25"], ["Jane", "30"]], 'output.csv', ["Name", "Age"], True, 'utf-8')

        # Check if open was called correctly
        mock_open.assert_called_once_with('output.csv', 'w', newline='', encoding='utf-8')

        # Check if csv.writer was called with the correct file object
        mock_csv_writer.assert_called_once_with(mock_open())

        # Check if writer.writerow and writer.writerows were called correctly
        mock_writer.writerow.assert_called_once_with(["Name", "Age"])
        mock_writer.writerows.assert_called_once_with([["John", "25"], ["Jane", "30"]])


if __name__ == '__main__':
    unittest.main()
