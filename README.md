cat_data.py: Concatenate data from a directory of .tsv files into a single file.

Usage: 
$ python cat_data.py input_directory output_file_name

Read the headers from all the .tsv (tab-separated values) files in the input
directory and write them to the output file, sorted, no duplicates. Then go
through all the files in the input directory again and write each row of input
data to the output file, placing each data value in the appropriate column.  Not
all columns are expected to be populated for any row.
