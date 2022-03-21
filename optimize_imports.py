import argparse
import logging
import os
from tqdm import tqdm


def optimize_inserts(file_path):
    logging.info(f'Processing File: {file_path}')
    f_name = file_path.split(".")[0]
    extension = "sql"

    save_path = os.path.join(os.getcwd(), f_name + "_o." + extension)
    logging.info(f"Writing to: {save_path}")

    # Open a blank file to write optimized SQL to
    f = open(save_path + "." + extension, "w+")

    try:
        # Opening the file
        num_lines = sum(1 for _ in open(file_path))
        with open(file_path) as fp:
            # Reading data line-by-line
            with tqdm(total=num_lines) as pbar:
                line = fp.readline()
                pbar.update(1)
                line_number = 1
                buffer = ""
                while line:
                    # process the line extracted
                    if is_insert_statement(line):
                        tokens = line.split(" ")
                        buffer_tokens = buffer.split(" ")
                        try:
                            if buffer.split(" ")[2] == tokens[2] and \
                                    " ".join(tokens[
                                             3:next(i for i, v in enumerate(tokens) if v.upper() == 'VALUES')]) == \
                                    " ".join(buffer_tokens[
                                             3:next(i for i, v in enumerate(buffer_tokens) if v.upper() == 'VALUES')]):
                                idx = next(i for i, v in enumerate(tokens) if v.upper() == 'VALUES')
                                values = " ".join(tokens[idx + 1:])
                                new_vals = sanitize_values(values)
                                buffer = combine_buffer(buffer, new_vals)
                            else:
                                f.write(buffer)
                                buffer = line
                        except IndexError:
                            buffer = line

                        line = fp.readline()
                        pbar.update(1)
                        line_number += 1
                    else:
                        f.write(line)
                        line = fp.readline()
                        pbar.update(1)
                        line_number += 1

                f.write(buffer)
    except IOError:
        logging.error("An error occurred trying to read the file.")
    finally:
        # Close the file to save changes
        f.close()


def sanitize_values (line):
    idx = line.rfind(')')
    out = line[:idx+1]
    return out


def combine_buffer(buffer, new_value):
    idx = buffer.rfind(')')
    buffer = buffer[:idx + 1] + ', ' + new_value + ';\n'
    return buffer


def is_insert_statement(line: str):
    key_words = ["insert", "INSERT"]
    out = list()
    for word in key_words:
        if line.__contains__(word):
            out.append((True, word))

    # Check if output list is not empty
    if len(out) == 0:
        # If list is empty return False
        return False
    else:
        return True


def setup_logging():
    logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    setup_logging()
    parser = argparse.ArgumentParser(description='Optimize INSERTTs in a SQL script')
    parser.add_argument(
        "files", metavar="f", type=str, nargs="*", help="provide filenames to optimize"
    )
    args = parser.parse_args()

    files = args.files
    if files:
        for file in files:
            optimize_inserts(file)
    else:
        logging.info("No files passed in to optimize!")
