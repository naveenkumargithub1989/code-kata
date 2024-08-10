import csv
import json
import codecs
import  os

def validate_encoding(encoding):
    """Check if the encoding is valid."""
    try:
        codecs.lookup(encoding)
        return True
    except LookupError:
        return False
def is_file_empty(file_path):
    """Check if the file is empty."""
    return os.path.getsize(file_path) == 0

def read_spec(file_path):
        """
        Reads the JSON specification file and extracts the necessary configuration.

        Args:
            file_path (str): The path to the JSON specification file.

        Returns:
            tuple: Contains column names, offsets, fixed-width encoding, include header flag, and delimited encoding,
            or None if there is an error.
        """
        try:
            if is_file_empty(file_path):
                print(f"Error: The file '{file_path}' is empty.")
                return None

            with open(file_path, 'r', encoding='utf-8') as spec_file:
                spec = json.load(spec_file)
                column_names = spec.get("ColumnNames", [])
                offsets = spec.get("Offsets", [])
                fixed_width_encoding = spec.get("FixedWidthEncoding", "")
                include_header = spec.get("IncludeHeader", "").lower()
                delimited_encoding = spec.get("DelimitedEncoding", "")

                # Validate that all offsets are integers
                if not all(offset.isdigit() for offset in offsets):
                    print("Error: All offsets must be numeric.")
                    return None
                offsets = [int(x) for x in offsets]

                # Validate encodings
                if not validate_encoding(fixed_width_encoding):
                    print(f"Error: Invalid FixedWidthEncoding: {fixed_width_encoding}")
                    return None

                # Validate IncludeHeader
                if include_header not in ["true", "false"]:
                    print(f"Error: Invalid IncludeHeader value: {include_header}")
                    return None

                if not validate_encoding(delimited_encoding):
                    print(f"Error: Invalid DelimitedEncoding: {delimited_encoding}")
                    return None

                return column_names, offsets, fixed_width_encoding, include_header, delimited_encoding
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
            return None
        except json.JSONDecodeError:
            print(f"Error: The file '{file_path}' is not a valid JSON file.")
            return None
        except KeyError as e:
            print(f"Error: Missing key in specification file: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while reading the spec: {e}")
            return None


def check_column_offset_match(column_names, offsets):
    """
    Checks if the number of column names matches the number of offsets.

    Args:
        column_names (list): The list of column names from the spec file.
        offsets (list): The list of offsets from the spec file.

    Returns:
        bool: True if the number of column names matches the number of offsets, False otherwise.
    """
    if len(column_names) != len(offsets):
        print("Error: The number of column names does not match the number of offsets.")
        return False
    return True

def parse_fixed_width(file_path, offsets, fixed_width_encoding):
    """
    Parses a fixed-width formatted file into a list of records based on the specified offsets.

    Args:
        file_path (str): The path to the fixed-width data file.
        offsets (list of int): List of field lengths for parsing the fixed-width file.
        fixed_width_encoding (str): Encoding of the fixed-width file.

    Returns:
        list: A list of parsed records.
    """
    try:
        if is_file_empty(file_path):
            print(f"Error: The file '{file_path}' is empty.")
            return None
        parsed_data = []
        with open(file_path, 'r', encoding=fixed_width_encoding) as data_file:
            for line in data_file:
                record = []
                position = 0
                for length in offsets:
                    field = line[position:position + length].strip()
                    record.append(field)
                    position += length
                parsed_data.append(record)
        return parsed_data
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while parsing the fixed-width file: {e}")
        return []

def write_to_csv(parsed_data, output_path, column_names, include_header, delimited_encoding):
    """
    Writes parsed data to a CSV file using the specified encoding and includes a header if specified.

    Args:
        parsed_data (list): The parsed data to be written to the CSV file.
        output_path (str): The path where the CSV file will be saved.
        column_names (list of str): The column names for the CSV file header.
        include_header (bool): Flag indicating whether to include a header in the CSV file.
        delimited_encoding (str): Encoding of the CSV file.
    """
    try:
        with open(output_path, 'w', newline='', encoding=delimited_encoding) as csv_file:
            writer = csv.writer(csv_file)
            if include_header:
                writer.writerow(column_names)
            writer.writerows(parsed_data)
        print(f"CSV file '{output_path}' created successfully.")
    except IOError as e:
        print(f"Error: Unable to write to file '{output_path}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while writing to the CSV file: {e}")

if __name__ == "__main__":
    spec = read_spec('spec.json')
    if spec:
        column_names, offsets, fixed_width_encoding, include_header, delimited_encoding = spec
        if check_column_offset_match(column_names, offsets):
            parsed_data = parse_fixed_width('customer_data_sample_1l.txt', offsets, fixed_width_encoding)
            if parsed_data:
                write_to_csv(parsed_data, 'output.csv', column_names, include_header, delimited_encoding)
