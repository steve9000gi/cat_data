#!/usr/bin/env python

""" cat_data.py: Concatenate data from a directory of .tsv files into a single file.

Read the headers from all the .tsv (tab-separated values) files in the input
directory and write them to the output file, sorted, no duplicates. Then go
through all the files in the input directory again and write each row of input
data to the output file, placing each data value in the appropriate column.  Not
all columns are expected to be populated for any row.
"""

import os
import fnmatch
import sys
import logging
import re


def uniquify_ordered_list(seq): 
    """ https://www.peterbe.com/plog/uniqifiers-benchmark: order preserving """
    seen = {}
    result = []
    for item in seq:
        if item in seen:
            continue
        seen[item] = 1
        result.append(item)
    return result


def get_all_strings_that_start_with(start_string, seq):
    """ Return list of all strings in seq that begin with start_string. """
    result = []
    for i in seq:
        if i.startswith(start_string):
            result.append(i)
    return result


def write_header(input_dir, output_file):
    """ Write out header consisting of all headers from all files in input_dir"""

    logger.error("1st pass: writing master header")

    out_stream = open(output_file, 'w')
    header = ['age', 'intervention', 'iteration', 'replication']

    for root, dirname, filename in os.walk(input_dir):
        for idx, file_name in enumerate(fnmatch.filter(filename, 'summary_agent.*')):
            path = os.path.join(root, file_name)
            stream = None
            #logger.error(path)
            try:
                stream = open(path, 'r')
                header_line = stream.readline()
                header_line = header_line.replace('\n', '').replace('\r','')
                header_parts = re.split(r'\t+', header_line)
                header = header + header_parts
            finally:
                if stream:
                    stream.close()

    header_uniquified = uniquify_ordered_list(header)
    fobt_list = sorted(get_all_strings_that_start_with("fobt_utd_year",
                                                        header_uniquified))
    col_list = sorted(get_all_strings_that_start_with("col_utd_year",
                                                      header_uniquified))
    other_list = [x for x in header_uniquified if x not in fobt_list]
    other_list = [x for x in other_list if x not in col_list]
    header_sorted = other_list + fobt_list + col_list
    header_string = '\t'.join(header_sorted)
    header_string += "\r\n"
    out_stream.write(header_string)
    return header_sorted


def align_values(line, master_header, curr_header):
    """ May be more columns in master header than in current header."""
    line = line.replace('\n', '').replace('\r','')
    data_row = line.split("\t")
    curr_dict = {}
    for i in range(len(data_row)):
        curr_header[i] = curr_header[i].replace('\n', '').replace('\r','') # hack
        curr_dict[curr_header[i]] = data_row[i]
        #if i == len(data_row) - 1:
        #    print(curr_header[i], ": ", curr_dict[curr_header[i]])
    master_dict = {}
    for i in range(len(master_header)):
        master_dict[master_header[i]] = None
    for i in range(len(curr_header)): 
        master_dict[curr_header[i]] = data_row[i] 
    master_line = ""
    for i in range(4, len(master_header)): # 1st 4 values already written out
        if master_dict[master_header[i]] != None:
            master_line += master_dict[master_header[i]]
        master_line += "\t"
    master_line += "\r\n"
    return master_line


def write_data(input_dir, output_file, master_header_list):
    """ Write data from all files in input directory to a single output file."""

    logger.error("2nd pass: writing data")

    out_stream = open(output_file, 'a')  # Append: header's already in there.
    header_has_been_read = False
    for root, dirname, filename in os.walk(input_dir):
        for idx, file_name in enumerate(fnmatch.filter(filename, 'summary_agent.*')):
            path = os.path.join(root, file_name)
            stream = None
            #logger.error(path)
            try:
                stream = open(path, 'r') 
                if not header_has_been_read: # ...then get current header
                    header = stream.readline()
                    current_header_list = header.split("\t")
                    #logger.error(current_header_list)
                    header_has_been_read = True
                else:
                    stream.readline()

                age = path.split('/')[-2].split('.')[2]
                iteration = path.split('/')[-2].split('.')[3]
                scenario = path.split('/')[-2].split('.')

                # account for control vs intervention format. 
                intervention = scenario[5] if len(scenario) > 5 else scenario[4]

                replication = path.split('/')[-1].split('.')[2]
                if 'control' == intervention:
                    intervention = -1

                for line in stream:
                    #logger.error(line)
                    out_stream.write("%s\t%s\t%s\t%s\t"
                        % (age, intervention, iteration, replication))
                    aligned_values = align_values(line,
                                                  master_header_list, 
                                                  current_header_list)
                    out_stream.write(aligned_values)
            finally:
                header_has_been_read = False
                if stream:
                    stream.close()


logger = logging.getLogger(__name__)
logging.basicConfig(level=3, format='%(asctime)-15s %(message)s')

in_dir = sys.argv[1]
out_file = sys.argv[2]

master_header = write_header(in_dir, out_file)
write_data(in_dir, out_file, master_header)

sys.exit(0)

