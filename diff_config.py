#!/usr/bin/python2.7
#
# March 2014

"""Analyze Unix configuration files across remote machines.

Show differences in a configurations from multiple machines by displaying each
directive's value in as friendly way as possible to highlight where
configurations differ from machine to machine.
"""

import os
import re
import argparse
import shutil
import getpass
import tempfile
import subprocess
import threading
import Queue
import pexpect

# TODO: webify the user interface


class ParseConfigFile(threading.Thread):
  """Parse out a configuration file."""

  def __init__(self, delimiter, tmp_dir, conf_instance):
    """Initializes the class with some constants.

    Args:
      queue: An int of the size of the threading queue.
      delimiter: A string of the key/value delimiter.
      tmp_dir: A string of the temporary directory.
      conf_instance: A string of the file name and machine name.
    """
    threading.Thread.__init__(self)
    self.delimiter = delimiter
    self.tmp_dir = tmp_dir
    self.conf_instance = conf_instance
    self.file_path = '%s/%s' % (self.tmp_dir, self.conf_instance)
    self.mach_name = ''
    self.directives = []
    self.effective_config = []
    self.delimiter_error = False

  def ParseFile(self):
    """Parse the configuration file."""
    with open(self.file_path) as file_obj:
      data = file_obj.readlines()
      for line in data:
        self.StoreKeyValue(line)

  def StoreKeyValue(self, line):
    """Split the wheat from the chaff.
    
    Args:
      line: A string of text.
    """
    line = line.strip()
    line = line.replace('\t', ' ')
    # eliminate lines that aren't meaningful configuration
    if re.match(r'[a-zA-Z0-9$]', line):
      if len(line.split(self.delimiter)) == 0:
        print 'This is highly irregular' # "Airplane 2"
      elif len(line.split(self.delimiter)) == 1:
        # this might happen when a delimiter is not provided
        self.directive = ''.join(line.split(self.delimiter)[0])
        self.detail = ''
        self.delimiter_error = True
      elif len(line.split(self.delimiter)) == 2:
        self.directive, self.detail = line.split(self.delimiter)
      elif len(line.split(self.delimiter)) >= 3:
        self.directive = ''.join(line.split(self.delimiter)[0])
        self.detail = self.delimiter.join(line.split(self.delimiter)[1:])

      self.directives.append(self.directive)
      appended_comments = re.compile(r'(?P<config>[^#;/!]*)[^#;/!]([;]|[#]|[!]'
                                      '|[/]{2}).*')
      # strip out where comments append the configuration text
      if appended_comments.match(self.detail):
        self.config_dict = appended_comments.match(self.detail).groupdict()
        line_conf = self.config_dict['config']
      else:
        line_conf = self.detail
      line_conf = line_conf.strip()
      config_tuple = (self.directive, line_conf)
      self.effective_config.append(config_tuple)

  def run(self):
    """The worker method."""
    self.mach_name = self.conf_instance.split('_')[-1]
    self.ParseFile()


class FetchRemoteConfig(threading.Thread):
  """Gather remote configuration files onto the localhost."""

  def __init__(self, queue, configfile, user, password, tmp_dir, verbose):
    """Initializes the class with some constants.

    Args:
      queue: An int of the size of the threading queue.
      password: A int of the size of the threading queue.
    """
    threading.Thread.__init__(self)
    self.queue = queue
    self.configfile = configfile
    self.user = user
    self.password = password
    self.tmp_dir = tmp_dir
    self.scp_template = ('/usr/bin/scp -q -o ConnectTimeout=3 -p %s@%s:%s '
                         '%s/%s')
    self.verbose = verbose

  def RetrieveFile(self):
    """Fetch the remote file over SSH."""
    if self.password:
      self.DoManualAuth()
    else:
      try:
        error = subprocess.Popen(self.scp_command, shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE).communicate()[1]
        if error and self.verbose is True:
          print '%s said: %s' % (self.mach, error.strip())
      except:
        print('could not fetch config file from %s' % self.mach)

  def DoManualAuth(self):
    """Pass an SSH session a user supplied password."""
    # if scp succeeds with the given password then an exception will be thrown
    # silently. otherwise if the password is wrong then just warn the user.
    try:
      self.ssh_conn = pexpect.spawn(self.scp_command, timeout=3)
      self.ssh_conn.expect('assword:')
      self.ssh_conn.sendline(self.password)
      i = self.ssh_conn.expect(['assword:', 'ermission denied',
                                'o such file or directory'])
      if i == 0:
        print 'your password failed on %s' % self.mach
        self.ssh_conn.close(force=True)
      elif i == 1 and self.verbose is True:
        print 'file inaccessible on %s' % self.mach
      elif i == 2 and self.verbose is True:
        print 'file does not exist on %s' % self.mach
    except Exception, err:
      if self.ssh_conn:
        self.ssh_conn.close(force=True)
    else:
      if self.ssh_conn:
        self.ssh_conn.close()

  def run(self):
    """The worker method."""
    while True:
      self.mach = self.queue.get()
      self.local_file = '%s_%s' % (self.configfile.replace('/', '_'), self.mach)
      self.scp_command = self.scp_template % (self.user, self.mach,
                                              self.configfile, self.tmp_dir,
                                              self.local_file)
      self.RetrieveFile()
      self.queue.task_done()


class ExecuteNetcat(threading.Thread):
  """The class to run a network connection."""

  def __init__(self, queue):
    """The method to set some constants.

    Args:
      queue: An int as the queue size.
    """
    threading.Thread.__init__(self)
    self.queue = queue
    self.nc_command = '/bin/nc -w 1 %s 22'
    self.short_banner = 'SSH-2.0-OpenSSH_'
    self.available_machs = []

  def RunNc(self):
    """Execute the network connection test."""
    check_command = self.nc_command % self.mach
    output = subprocess.Popen(check_command, shell=True,
                              stdout=subprocess.PIPE).communicate()[0]
    if output.startswith(self.short_banner):
      self.available_machs.append(self.mach)

  def run(self):
    """The worker method."""
    while True:
      self.mach = self.queue.get()
      self.RunNc()
      self.queue.task_done()


def CommaSeparateValues(value):
  """Comma separate the values.

  Args:
    value: A string of comma separated values.
 
  Returns:
    csv_list: A list of values.
  """
  csv_list = map(str, value.split(','))
  return csv_list 


def CheckReachAbility(machines):
  """Establish reachability for each remote machine.

  Args:
    machines: A list of machine names.

  Returns:
    results: A dict of ping results.
    reachable: A list of machines reachable by ICMP.
    unreachable: A list of machines not reachable by ICMP.
  """
  fping_command = ('/usr/bin/fping -r 1 -t 1000 %s 2> /dev/null' % 
                   ' '.join(machines))
  output = subprocess.Popen(fping_command, shell=True,
                            stdout=subprocess.PIPE).communicate()[0]
  reachable = []
  matcher = r'(?P<mach>.*) is alive$'
  for match in re.finditer(matcher, output, re.MULTILINE):
    match_dict = match.groupdict()
    reachable.extend(match_dict.values())

  unreachable = [down for down in machines if down not in reachable]
  return reachable, unreachable


def PrintPretty(threadlist, color, machines):
  """Print the output in pure style.

  Args:
    threadlist: A list of thread objects.
    color: A boolean of whether to include colored output.
    machines: A list of machines.
  """
  heldback_output = False
  for thread in threadlist:
    file_name = os.path.basename(thread.file_path)
    conf_file = '/'.join(file_name.split('_')[:-1])
    bold_header = '=== ' + '\033[1m' + conf_file + '\033[0m'
  print '\n', bold_header, '\n'

  all_directives = []
  max_mach_len = 0
  for thread in threadlist:
    if thread.delimiter_error:
      heldback_output = True
      break
    for directive in thread.directives:
      if directive not in all_directives:
        all_directives.append(directive)
    if len(thread.mach_name) > max_mach_len:
      max_mach_len = len(thread.mach_name)
  max_mach_len += 2
  all_directives.sort()

  tag_for_color = []
  directive_counter_dict = {}
  for sorted_directive in all_directives:
    directive_counter_dict[sorted_directive] = 0
    details = []
    for thread in threadlist:
      thread_directives = []
      for config_item in thread.effective_config:
        a_directive, detail = config_item
        details.append(detail)
        thread_directives.append(a_directive)
      # add a warning counter for the directive when its absent on this machine
      if sorted_directive not in thread_directives:
        directive_counter_dict[sorted_directive] += 1

    for item in details:
      if (details.count(item) == 1 and item not in tag_for_color):
        if len(machines) > 1:
          tag_for_color.append(item)

  for sorted_directive in all_directives:
    if directive_counter_dict[sorted_directive] > 0:
      if color is False:
        print (sorted_directive + ' (' +
               str(directive_counter_dict[sorted_directive]) + ')')
      else:
        print (sorted_directive + ' (' + '\033[93m' + 
               str(directive_counter_dict[sorted_directive]) +
               '\033[0m') + ' missing: )'
    else:
      print sorted_directive

    for thread in threadlist:
      if thread.delimiter_error:
        heldback_output = True
        break
      for config_item in thread.effective_config:
        a_directive, detail = config_item
        if a_directive == sorted_directive:
          spacer = '%' + str(max_mach_len) + 's'
          mach_output = spacer % thread.mach_name
          output = '%s  %s' % (mach_output, detail)
          if detail in tag_for_color:
            if color is False:
              print output
            else:
              print '\033[91m' + output + '\033[0m'
          else:
            print output
  if heldback_output:
    print 'Output was held back. Try using or removing the delimiter flag.'
  print ''


def main():
  parser = argparse.ArgumentParser(description='Compare Unix config files.')
  parser.add_argument('configfiles', metavar='FILE', nargs='*',
                      help=('this should be the absolute path name of the '
                            'configuration file being compared'))
  parser.add_argument('-m', '--machines', type=CommaSeparateValues,
                      help='A comma separated list of machines to compare the '
                      'configuration file from')
  parser.add_argument('-d', '--delimiter', help='Force a delimiter to separate '
                      'the key/value pair in the config file. Defaults to a '
                      'space.')
  parser.add_argument('-u', '--user', help='A user to authenticate against '
                      'remote machines')
  parser.add_argument('-p', '--password', action='store_true', help='Use '
                      'one password for authenticating against all machines')
  parser.add_argument('-n', '--nocolor', action='store_false',
                      help='Disable colored output')
  parser.add_argument('-v', '--verbose', action='store_true',
                      help='Include error messages with the output')
  args = parser.parse_args()

  if args.configfiles:
    configfiles = args.configfiles
  else:
    parser.error('An argument is required')

  machines = args.machines
  if machines:
    for mach in machines:
      if '_' in mach:
        parser.error('Invalid character in machine name')
    if args.user:
      user = args.user
    else:
      user = getpass.getuser()
    if args.password:
      password = getpass.getpass('Password: ')
    else:
      # a case when your SSH keys are already propagated across machines
      password = ''

  if args.delimiter:
    delimiter = args.delimiter
  else:
    delimiter = ' '

  threadlist = []
  if machines:
    reachable, unreachable = CheckReachAbility(machines)
    nc_queue = Queue.Queue()
    if len(reachable) <= 10:
      nc_queue_len = len(reachable)
    else:
      nc_queue_len = 10
    for nc_conn in range(nc_queue_len):
      nc_thread = ExecuteNetcat(nc_queue)
      nc_thread.setDaemon(True)
      threadlist.append(nc_thread)
      nc_thread.start()
    for mach in reachable:
      nc_queue.put(mach)
    nc_queue.join()

  available_machs = []
  for thread in threadlist:
    available_machs.extend(thread.available_machs)
  print('Proceeding on %d machines: %s' % (len(available_machs),
                                           ', '.join(available_machs)))

  tmp_paths = []
  for configfile in configfiles:
    if available_machs:
      tmp_dir = tempfile.mkdtemp()
      tmp_paths.append(tmp_dir)
      scp_queue = Queue.Queue()
      # please enjoy ssh connections responsibly
      scp_queue_len = 5
      threadlist = []
      for scp_conn in range(scp_queue_len):
        scp_thread = FetchRemoteConfig(scp_queue, configfile, user, password,
                                       tmp_dir, args.verbose)
        scp_thread.setDaemon(True)
        threadlist.append(scp_thread)
        scp_thread.start()
      for mach in available_machs:
        scp_queue.put(mach)
      scp_queue.join()

  for tmp_dir in tmp_paths:
    threadlist = []
    for conf_instance in os.listdir(tmp_dir):
      current_thread = ParseConfigFile(delimiter, tmp_dir, conf_instance)
                                       
      current_thread.start()
      current_thread.join()
      threadlist.append(current_thread)
    if threadlist:
      PrintPretty(threadlist, args.nocolor, machines)
  try:
    [shutil.rmtree(path) for path in tmp_paths]
  except:
    print 'cleanup failed'


if __name__ == '__main__':
  try:
    main()
  except (KeyboardInterrupt, EOFError):
    print('\nHalting')
