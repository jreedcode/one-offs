#!/usr/bin/python2.6
#
# jreedcode@gmail.com

"""A simple shell wrapper for SQLite.

Allow quicker database interaction with SQLite files in the current directory.
Avoids relying on tab completion to complete names for SQLite files which may
include a custom extension. Prints a cleanly spaced command output (terminal
width permitting) to the screen.
"""

import os
import re
import subprocess
from optparse import OptionParser
import sqlite3

DATABASE_SUFFIX = '.db'
#DATABASE_SUFFIX = 'Login Data'


def GetDbFiles(no_suffix):
  """Get a list of database files from the current working directory.

  Args:
    no_suffix: A boolean to indicate if sqlite files might not have extensions.
  
  Returns:
    db_files: A list of sqlite database files to query.
  """
  if no_suffix:
    files_to_query = os.listdir(os.getcwd())
  else:
    files_to_query = [filename for filename in os.listdir(os.getcwd()) if
                      filename.endswith('%s' % DATABASE_SUFFIX)]

  db_files = []
  for a_file in files_to_query:
    try:
      output = subprocess.Popen('/usr/bin/file %s' % a_file, shell=True,
                                stdout=subprocess.PIPE).communicate()[0]
    except OSError:
      print 'cannot determine file type'
      output = ''
    except ValueError:
      print 'cannot determine file type'
      output = ''
    except Exception, err:
      print 'cannot determine file type %s' % str(err)
      output = ''
    file_results = output.rstrip('\n')
    if re.search(r'(s|S)(q|Q)(l|L)ite', file_results):
      db_files.append(a_file)
  return db_files
 

def PrintSchema(db_files):
  """Print to the screen each database schema.

  Args:
    db_files: A list of sqlite database files to print metadata for.
  """
  for db_file in db_files:
    try:
      conn = sqlite3.connect('%s' % db_file)
      with conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sqlite_master')
        results = cursor.fetchall()
        print '%s' % db_file
        for row in results:
          data_type = row[0]
          object_name = row[1]
          object_data = row[4]
          if data_type == 'table':
            print '%s\n  %s' % (object_name, object_data)
    except sqlite3.Error:
      print 'could not open %s' % db_file
      continue
  return


def ProcessSql(db_files, sql_command, term_width):
  """Process an SQL command against the database.

  Args:
    db_files: A list of sqlite database files to print metadata for.
    sql_command: A string of the SQL command.
    term_width: An int of the expected terminal width.
  """
  for db_file in db_files:
    try:
      conn = sqlite3.connect('%s' % db_file)
      with conn:
        cursor = conn.cursor()
        cursor.execute(sql_command)
    except sqlite3.OperationalError:
      print 'command failed. quote your arguments or escape special chars.'
      continue
    except:
      print 'command failed. could not open %s' % db_file
      continue
    print '%s' % db_file
    sql_results = cursor.fetchall()
    PrintSqlResults(sql_results, term_width)
  return


def PrintSqlResults(sql_results, readable_length):
  """Print the SQL results as friendly as the terminal width permits.

  Args:
    sql_results: A list of tuple of the SQL results.
    term_width: An int of the expected terminal width.
  """
  max_length = 0
  for line in sql_results:
    line_length = [sum(len(str(x)) for x in line)]
    if line_length[0] > max_length:
      max_length = line_length[0]
 
  # test if line formatting should proceed
  if max_length < readable_length:
    max_lengths_list = []
    for line in sql_results:
      for element in line:
        # initialize list elements with zero
        max_lengths_list.append('%d' % 0)
      break

    for line in sql_results:
      index_num = 0
      for element in line:
        string_length = len(str(element))
        if string_length > int(max_lengths_list[index_num]):
          max_lengths_list[index_num] = string_length
        index_num += 1

    list_of_spacers = []
    for max_len in max_lengths_list:
      list_of_spacers.append('%' + '%s' % max_len + 's')
    string_spacer = ' '.join(list_of_spacers)

    for line in sql_results:
      print string_spacer % line

  else:
    for line in sql_results:
      print line
  return


def main():
  usage = """
  %prog [OPTION] SQL_COMMAND

Print data from SQLite databases in the current directory.
  """
  parser = OptionParser(usage=usage)
  parser.add_option('-s', '--schema', dest='list_schema', default=False,
                    action='store_true',
                    help=('Print all database schema info.'))
  parser.add_option('-n', '--no-suffix', dest='no_suffix', default=False,
                    action='store_true',
                    help=('Ignore the default suffix and use the shell to '
                          'determine if each file is an SQLite database.'))
  parser.add_option('-w', '--width', dest='term_width', type='int',
                    default=200, help=('The max characters your terminal width '
                                       'will support.'))
  (options, args) = parser.parse_args() 
  sql_command = ' '.join(args)
 
  db_files = GetDbFiles(options.no_suffix)
  if db_files:
    if options.list_schema:
      PrintSchema(db_files)
    else:
      if sql_command:
        ProcessSql(db_files, sql_command, options.term_width)


if __name__ == '__main__':
  main()
