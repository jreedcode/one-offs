#!/usr/bin/python
#
# Jan 2012

"""Display the shell commands used to rebuild file permissions.

The most useful scenario is running this from a mirror system to gather the
permission set and then applying to them to another.
"""

import os
import sys
import grp
import pwd
import getopt
import threading
import platform

PRINT_TEMPLATE = """
  %d files.
"""


class LearnFileAttributes(threading.Thread):
  """Learn file metadata."""

  def __init__(self, path, unix_file):
    """Inits the class with values for each file and it's path.

    Args:
      path: A string of the unix path.
      unix_file: A string of unix file name.
    """
    threading.Thread.__init__(self)
    self.path = path
    self.unix_file = unix_file
    if platform.system() == 'AIX':
      self.chown_path = '/usr/bin/chown'
      self.chmod_path = '/usr/bin/chmod'
    else:
      self.chown_path = '/bin/chown'
      self.chmod_path = '/bin/chmod'
    self.perm_commands = []

  def DetermineOwnership(self):
    """Find out which user and group own the file."""
    stat_info = os.stat('%s/%s' % (self.path, self.unix_file)) 
    uid = stat_info.st_uid
    gid = stat_info.st_gid
    self.alpha_uid = pwd.getpwuid(uid)[0]
    self.alpha_gid = grp.getgrgid(gid)[0]

  def DetermineMode(self):
    """Find out what permission bits are set."""
    self.mode = oct(os.stat('%s/%s' % (self.path, self.unix_file)).st_mode)[-4:]

  def BuildCommands(self):
    """Build the permission commands."""
    full_chown_cmd = '%s %s.%s %s/%s' % (self.chown_path, self.alpha_uid,
                                         self.alpha_gid, self.path,
                                         self.unix_file)
    full_chmod_cmd = '%s %s %s/%s' % (self.chmod_path, self.mode, self.path,
                                      self.unix_file)
    self.perm_commands.append(full_chown_cmd)
    self.perm_commands.append(full_chmod_cmd)

  def run(self):
    """The worker method."""
    self.DetermineOwnership()
    self.DetermineMode()
    self.BuildCommands()


def Usage(detailed_error='', print_help=False, quit=False):
  """A generic usage function.

  Args:
    detailed_error: A string of the error with some explaination.
    print_help: Boolean value to print the help menu.
    quit: Boolean to terminate execution.
  """
  if print_help:
    print """%s
Usage: %s -p [PATH] -f [FILES]
    Options:
              -h, --help:   This menu.
              -p, --path:   The path where the check the permissions.
              -f, --files:  A space seperated string of files.

    Examples:
              Query a path.
                $ ./%s -p /usr/bin -f "`ls -1 /usr/bin | xargs`"
  """ % (detailed_error, os.path.basename(sys.argv[0]),
         os.path.basename(sys.argv[0]))
  else:
    print '%s' % detailed_error
  if quit:
    sys.exit(1)


def PrintOutput(num_of_files, perm_cmd_list):
  """Print the permission commands to the screen.

  Args:
    num_of_files: An integer of the number of files.
    perm_cmd_list: A list containing lists of permission commands.
  """
  for file_commands in perm_cmd_list:
    for command in file_commands:
      print '%s' % command 
  print PRINT_TEMPLATE % num_of_files
  return


def main(argv):
  # not sure why I used getopt here *shrugs*
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'hp:f:', ['help', 'path=',
                                                       'files='])
  except getopt.GetoptError:
    Usage('Error parsing command line flags.', print_help=True, quit=True)
  for opt, arg in opts:
    if opt in ('-h', '--help'):
      Usage(print_help=True, quit=True)
    elif opt in ('-p', '--path'):
      fs_path = arg
    elif opt in ('-f', '--files'):
      files = arg.split()
    else:
      Usage('Unhandled option \'%s\'.' % arg, print_help=True, quit=True)

  # make sure we can continue
  try:
    if files and fs_path:
      path_exists = os.path.exists(fs_path) 
  except NameError:
    Usage('Your path or file(s) are missing.', print_help=True, quit=True)

  if path_exists:
    stripped_path = os.path.abspath(fs_path)
    threadlist = []
    for unix_file in files:
      if os.path.exists('%s/%s' % (stripped_path, unix_file)):
        current_thread = LearnFileAttributes(stripped_path, unix_file)
        threadlist.append(current_thread)
  else:
    Usage('Your path \'%s\' was not found.' % fs_path, quit=True)

  for thread in threadlist:
    thread.start()
  # wait for all threads to complete
  for thread in threadlist:
    thread.join()
 
  num_of_files = 0
  perm_cmd_list = []
  for thread in threadlist:
    perm_cmd_list.append(thread.perm_commands)
    num_of_files += 1

  PrintOutput(num_of_files, perm_cmd_list)
 

if __name__ == '__main__':
  try:
    main(sys.argv[1:])
  except KeyboardInterrupt:
    Usage('\n^C Bailing.', quit=True)
