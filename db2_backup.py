#!/usr/bin/python2.6
#
# backup db2

"""Take an online backup of a DB2 database.

Creates and optionally verifies a backup image and all archive log files. Keeps
all files rotated and sends an email on completion. Set the LOGRETAIN option to
RECOVERY in your database configuration. Tested on Unix/Linux with DB2 9.7.5.
"""
 
import os
import sys
import time
import re
import optparse
import syslog
import getpass
import subprocess
import smtplib

## Constants that likely need tweaking for your setup
# aka instance owner
DB_USER = 'db2inst1'
# the number of backup files to keep before starting this backup
BACKUPS_TO_KEEP = 1
DB_NAME = ''
DOMAIN_NAME = ''
EMAIL_LIST = []
SMTP_SERVER = ''
SHELL_PATH = '/bin/bash'

## Constants you likely dont need to change
BACKUP_COMMAND = ('backup database %s ONLINE to %s compress INCLUDE LOGS '
                  'without prompting')
SUFFIX_LOG_PATH = '%s/%s/NODE0000' % (DB_USER.lower(), DB_NAME.upper())
DB2_BIN = '/home/db2inst1/sqllib/bin/db2'
CHECK_LOG_BIN = '/home/db2inst1/sqllib/bin/db2cklog'
CHECK_IMAGE_BIN = '/home/db2inst1/sqllib/bin/db2ckbkp'
DB2_INSTALL_DIR = '/opt/IBM/db2/V9.7'
CONNECT_CMD = 'connect to %s' % DB_NAME
DISCONNECT_CMD = 'connect reset'
CONNECTED_MATCHERS = [ r'Database Connection Information',
                       r'SQL authorization ID += %s' % DB_USER.upper(),
                       r'Local database alias += %s' % DB_NAME.upper()]
LIST_HISTORY = 'list history backup since %s for %s'
GET_CONF = 'get db cfg for %s' % DB_NAME
DB_BACKUP_MATCHER = (r'^%s\.\d{1}\.%s\.NODE\d{4}\.CATN\d{4}\.(?P<backup_date>'
                     '\d{14})\.\d{3}' % (DB_NAME.upper(), DB_USER))
ARCHIVE_METHOD_MATCHER = r'.*\(LOGARCHMETH1\) = DISK:(?P<log_path>.*)'
LOGS_MATCHER = (r'.*%s.*S(?P<earliest_log>\d{7}).LOG.*S(?P<current_log>\d{7}).'
               'LOG')
LOG_FILE_MATCHER = r'S(?P<incrementor>\d{7}).LOG'
RETURN_CODE_MATCHER = (r'"db2cklog": .*"S(?P<log_file>\d{7}).LOG".* Return '
                       'code: "(?P<return_code>\d{1,})".')
POSITIVE_RESULT_TEMPLATE = 'Image Verification Complete - successful.'
IMAGE_MATCHER = (r'Backup successful. The timestamp for this backup image '
                 'is : (?P<backup_timestamp>\d{14})')
DU_COMMAND = '/usr/bin/du -sm %s/%s'
DU_MATCHER = r'(?P<mb>\d{1,})\.(?P<kb>\d{2}).*%s/%s'
DRAFT_MSG = []


def CheckDbState(connect_cmd, disconnect_cmd):
  """Test the db is up by connecting to it.

  Args:
    connect_cmd: A string of the command to open a connection to the db.
    disconnect_cmd: A string of the command to close a connection to the db.
  """
  full_connect_cmd = '%s %s' % (DB2_BIN, connect_cmd)
  connect_results = subprocess.Popen(full_connect_cmd, shell=True,
                                     stdout=subprocess.PIPE).communicate()[0]
  num_of_matches = []
  for matcher in CONNECTED_MATCHERS:
    if re.search(matcher, connect_results):
      num_of_matches.append('matched on %s' % matcher)

  if len(num_of_matches) != len(CONNECTED_MATCHERS):
    DRAFT_MSG.append('Unable to connect to %s. Aborting.' % DB_NAME.upper())
    SendEmail(DRAFT_MSG, 'FAILED')
    WriteToSyslog('Unable to connect to %s. Aborting.' % DB_NAME.upper(),
                  quit=True)
  else:
    full_disconnect_cmd = '%s %s' % (DB2_BIN, disconnect_cmd)
    disconnect_results = subprocess.Popen(full_disconnect_cmd, shell=True,
                           stdout=subprocess.PIPE).communicate()[0]
    return


def GetLogPathRoot(suffix_log_path):
  """Get the full log path of the db.

  Args:
    suffix_log_path: A string of the suffix part to a log path.
  Returns:
    log_path_root: A string of the archive log path root.
  """
  full_get_cfg_cmd = '%s %s' % (DB2_BIN, GET_CONF)
  archive_location = subprocess.Popen(full_get_cfg_cmd, shell=True,
                                      stdout=subprocess.PIPE).communicate()[0]
  for match in re.finditer(ARCHIVE_METHOD_MATCHER, archive_location,
                           re.MULTILINE):
    match_dict = match.groupdict()
    log_arch1_path = '%s' % (match_dict['log_path'],)
  log_path_root = '%s%s' % (log_arch1_path, suffix_log_path)
  return log_path_root


def FindRecentLogPath(log_path_root):
  """Find the current usable active db2 log path.
  
  Args:
    log_path_root: A string of the archive log path root.

  Returns:
    current_archive_path: A string of the currently used archive log directory.
  """
  log_directories = os.listdir(log_path_root)
  mtimes_dir_dict = {}
  for directory in log_directories:
    stat_data = os.stat('%s/%s' % (log_path_root, directory))
    mtimes_dir_dict[stat_data.st_mtime] = directory

  most_recent_mtime = max(mtimes_dir_dict.keys())
  current_archive_path = '%s/%s' % (log_path_root.rstrip('/'),
                                    mtimes_dir_dict[most_recent_mtime])
  WriteToSyslog('DB2 current log path is %s' % current_archive_path)
  return current_archive_path


def DeleteOldBackupFiles(path, backups_to_keep):
  """Delete database backup files.

  Args:
    path: A string as a filesystem path.
    backups_to_keep: An integer of days of backups files.
  Returns:
    dbs_to_keep: A list of database files being stored.
  """
  if os.path.exists(path):
    all_files = os.listdir(path)
  else:
    DRAFT_MSG.append('Failing with no place to save the backup')
    SendEmail(DRAFT_MSG, 'FAILED')
    WriteToSyslog('Failing with no place to save the backup', quit=True)

  db_files = []
  for one_file in all_files:
    if re.match(DB_BACKUP_MATCHER, one_file):
      db_files.append(one_file)

  dbs_to_keep = []
  if len(db_files) == 0:
    WriteToSyslog('There are no backup files to remove. Continuing.')
    return dbs_to_keep

  dates = []
  for db_file in db_files:
    match_object = re.match(DB_BACKUP_MATCHER, db_file)
    file_date_dict = match_object.groupdict()
    dates.append(int(file_date_dict['backup_date']))

  while len(dbs_to_keep) < backups_to_keep:
    if len(dates):
      newest = str(max(dates))
      for db_file in db_files:
        if newest == db_file.split('.')[-2]:
          dbs_to_keep.append(db_file)
      dates.remove(int(newest))
    else:
      break

  dbs_to_remove = [db for db in db_files if db not in dbs_to_keep]
  if dbs_to_remove:
    try:
      [os.remove('%s/%s' % (path, bak_file)) for bak_file in 
         dbs_to_remove]
    except Exception, err:
      WriteToSyslog('Could not remove %s for %s' % (bak_file, str(err)))
      dbs_to_remove.remove(bak_file)
    else:
      WriteToSyslog('Removed %s' % ' '.join(dbs_to_remove))

  dbs_to_keep = ['%s/%s' % (path, db) for db in dbs_to_keep]
  return dbs_to_keep
 

def DeleteOldLogFiles(dbs_to_keep, log_path_root):
  """Delete unused db2 log files.

  Args:
    dbs_to_keep: A list of database files being stored.
    log_path_root: A string of the archive log path root.

  Returns:
    files_to_keep: A list of log files being kept for roll forwards.
  """
  # get the earliest log file for the oldest backup
  backup_dates = [int(the_date.split('.')[-2]) for the_date in dbs_to_keep]
  oldest_backup_date = min(backup_dates)
  list_hist_cmd = LIST_HISTORY % (oldest_backup_date, DB_NAME)
  full_list_hist_cmd = '%s %s' % (DB2_BIN, list_hist_cmd)
  list_hist_output = subprocess.Popen(full_list_hist_cmd, shell=True, 
                                      stdout=subprocess.PIPE).communicate()[0]
  for database in dbs_to_keep:
    if str(oldest_backup_date) in database:
      time_stamp_seq_num = database.split('.')[-2:]
      time_stamp_seq_num = ''.join(time_stamp_seq_num)

  # initialize this in case there is no re match  
  early_log_incrementor = 1
  for match in re.finditer(LOGS_MATCHER % time_stamp_seq_num, list_hist_output,
                           re.MULTILINE):
    match_dict = match.groupdict()
    early_log_incrementor = int('%s' % (match_dict['earliest_log'],))

  files_to_remove = []
  files_to_keep = []

  for db2_log_dir in os.listdir(log_path_root):
    current_log_path = '%s/%s' % (log_path_root, db2_log_dir)

    log_files = os.listdir(current_log_path)
    for log_file in log_files:
      if re.match(LOG_FILE_MATCHER, log_file):
        match_object = re.match(LOG_FILE_MATCHER, log_file)
        match_dict = match_object.groupdict()
        current_incrementor = int('%s' % (match_dict['incrementor'],))
        # leave a buffer of one extra log file
        if current_incrementor < (early_log_incrementor - 1):
          files_to_remove.append('%s/%s' % (current_log_path, log_file))
        else:
          files_to_keep.append('%s' % log_file)
 
  for log_file in files_to_remove:
    try:
      os.remove(log_file)
    except Exception, err:
      WriteToSyslog('Could not remove %s for %s' % (log_file, str(err)))
      files_to_remove.remove(log_file)

  removed_log_names = [log.split('/')[-1] for log in files_to_remove]
  removed_log_names.sort()
  files_to_keep.sort()
  WriteToSyslog('Removed %d logs: %s' % (len(removed_log_names),
                                         ' '.join(removed_log_names)))
  WriteToSyslog('Keeping %d logs: %s' % (len(files_to_keep),
                                         ' '.join(files_to_keep)))
  return files_to_keep
 

def VerifyLogFiles(current_archive_path, saved_log_files):
  """Verify the DB2 log files are valid to roll forward with.

  Args:
    current_archive_path: A string of the currently used archive log directory.
    saved_log_files: A list of log files being kept for roll forwards.
  """
  # lets avoid using a range and test each file seperately
  if len(saved_log_files) == 0:
    WriteToSyslog('Cannot verify log files')
    return

  clean_log_files = []
  # a dict for failed log files
  file_retcode_dict = {}
  for log_file in saved_log_files:
    log_number = log_file.rstrip('.LOG').lstrip('S0')
    check_cmd = '%s %s ARCHLOGPATH %s' % (CHECK_LOG_BIN, log_number,
                                          current_archive_path)
    check_log_results = subprocess.Popen(
                          check_cmd, shell=True,
                          stdout=subprocess.PIPE).communicate()[0]
    for match in re.finditer(RETURN_CODE_MATCHER, check_log_results,
                             re.MULTILINE):
      log_results_dict = match.groupdict()
      if log_results_dict['return_code'] == '0':
        clean_log_files.append('S%s.LOG' % (log_results_dict['log_file'],))
      else:
        bad_log_file = 'S%s.LOG' % (log_results_dict['log_file'],)
        return_code = '%s' % (log_results_dict['return_code'],)
        file_retcode_dict[bad_log_file] = return_code

  if len(file_retcode_dict) > 0:
    WriteToSyslog('%d logs failed verification: %s' % file_retcode_dict)
  if len(clean_log_files) > 0:
    WriteToSyslog('Verified %d clean logs: %s' % (len(clean_log_files),
                                                  ' '.join(clean_log_files)))
  return


def VerifyImage(path, timestamp):
  """Verify the backup image.
  
  Args:
    path: A string as a filesystem path.
    timestamp: A string of the backup's timestamp.

  Returns:
    image_verified: A boolean indicating successful image verification.
  """
  image_verified = False
  current_image = ''
  delimit_timestamp = '.%s.' % timestamp
  for image_file in os.listdir(path):
    if re.search(delimit_timestamp, image_file):
      current_image = image_file

  if current_image:
    check_image_cmd = '%s %s/%s' % (CHECK_IMAGE_BIN, path, current_image)
    verify_results = subprocess.Popen(check_image_cmd, shell=True,
                                      stdout=subprocess.PIPE).communicate()[0]
    if re.search(POSITIVE_RESULT_TEMPLATE, verify_results):
      image_verified = True
      WriteToSyslog('Image verification completed successfully')

  return image_verified


def BackupDb2(path):
  """Backup the database.

  Args:
    path: A string as a filesystem path.
  Returns:
    done_time: A float of the time in seconds it took to run the backup.
    timestamp: A string of the backup's timestamp.
  """
  command_options = BACKUP_COMMAND % (DB_NAME, path)
  full_command = '%s %s' % (DB2_BIN, command_options)
  WriteToSyslog('Running with "%s"' % full_command)
  # initialize the timestamp as not true
  timestamp = ''
  backup_start_time = time.time()
  try:
    backup_results = subprocess.Popen(full_command, shell=True,
                                      stdout=subprocess.PIPE).communicate()[0]
  except Exception, err:
    backup_results = str(err)
  else:
    done_time = time.time() - backup_start_time
    for match in re.finditer(IMAGE_MATCHER, backup_results, re.MULTILINE):
      match_dict = match.groupdict()
      timestamp = '%s' % (match_dict['backup_timestamp'],)

  trimmed_results = backup_results.strip()
  WriteToSyslog('Backup completed with "%s"' % trimmed_results)
  return done_time, timestamp


def SendEmail(draft_msg, completed_status):
  """Email out the status.

  Args:
    draft_msg: A list of status messages collected throughout the script.
    completed_status: A string indicating success or failure. 
  Output:
    An email to those in the know.
  """
  subject = '%s DB2 backup: %s' % (os.uname()[1].lower(), completed_status)
  from_addr = '%s@%s' % (DB_USER, DOMAIN_NAME)
  header = ('From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n' % (from_addr,
                                                           'db2-owners',
                                                           subject))
  full_body = '\n'.join(draft_msg)
  header_and_body = header + '\n' + full_body
  try:
    mail_server = smtplib.SMTP(SMTP_SERVER)
    mail_server.sendmail(from_addr, EMAIL_LIST, header_and_body)
    mail_server.quit()
  except Exception, err:
    WriteToSyslog('%s failed to send it\'s email %s.' % (sys.argv[0],
                                                         str(err)))
  else:
    WriteToSyslog('Email sent')
  return


def CalculateBackupSize(path, timestamp):
  """Determine the size of the backup taken.

  Args:
    path: A string as a filesystem path.
    timestamp: A string of the backup's timestamp.
  """
  db_backup_files = os.listdir(path)
  for db_file in db_backup_files:
    if timestamp in db_file:
      # use the os because module os.stat cannot handle large files
      du_command = DU_COMMAND % (path, db_file)
      du_results = subprocess.Popen(du_command, shell=True,
                                    stdout=subprocess.PIPE).communicate()[0]
      du_matcher = DU_MATCHER % (path, db_file)
      for match in re.finditer(du_matcher, du_results, re.MULTILINE):
        match_dict = match.groupdict()
        megabytes = '%s' % (match_dict['mb'],)
        kilobytes = '%s' % (match_dict['kb'],)
        WriteToSyslog('The file was saved to %s/%s with a size of %s.%s MB' 
                      % (path, db_file, megabytes, kilobytes))
  return


def WriteToSyslog(message, store_to_draft=True, quit=False):
  """Write a message to syslog and save it to a list.

  Args:
    message: A string of text to send to syslog.
    store_to_draft: A boolean to ignore a message for more than just syslog.
    quit: A boolean telling this function to quit after throwing a message.
  """
  syslog.openlog(sys.argv[0], syslog.LOG_PID, syslog.LOG_DAEMON)
  syslog.syslog(syslog.LOG_ERR, '%s.' % message)
  syslog.closelog()
  if store_to_draft:
    DRAFT_MSG.append(message)
  if quit:
    sys.exit(1)
  return


def RecordDuration(backup_duration, main_start_time):
  """Calculate the times it took to run.

  Args:
    backup_duration: A float of the backup time.
    main_start_time: A float as a sum of all functions times.
  """
  backup_total_minutes = '%.0f' % (backup_duration / 60)
  backup_seconds_past_minutes = '%.0f' % (backup_duration % 60)

  sum_of_seconds = time.time() - main_start_time
  total_minutes = '%.0f' % (sum_of_seconds / 60)
  total_seconds_past_minutes = '%.0f' % (sum_of_seconds % 60)
  WriteToSyslog('The backup took %smins %ssecs while the script ran for %smins '
                 '%ssecs' % (backup_total_minutes, backup_seconds_past_minutes, 
                             total_minutes, total_seconds_past_minutes))
  return


def SourceDb2():
  """Source some environmental variables for interacting with DB2."""
  os.environ['DB2DIR'] = '%s' % DB2_INSTALL_DIR
  os.environ['DB2INSTANCE'] = '%s' % DB_USER
  os.environ['SHELL'] = '%s' % SHELL_PATH
  return


def main():
  if getpass.getuser() == DB_USER:
    WriteToSyslog('Starting DB2 Backup of %s' % DB_NAME.upper())
  else:
    WriteToSyslog('You need to be %s to run a backup' % DB_USER, quit=True)

  usage = """Usage: %prog [OPTIONS] path"""
  parser = optparse.OptionParser(usage=usage)
  parser.add_option('-l', '--log-verify', dest='logverify', default=False,
                    action='store_true', help='Verify DB2 archive log files')
  parser.add_option('-i', '--image-verify', dest='imageverify', default=False,
                    action='store_true', help='Verify the DB2 backup image')
  (options, args) = parser.parse_args()

  if not args:
    parser.error('Missing path. Use "-h" for help.')
  if len(args) > 1:
    parser.error('Bad path. Use "-h" for help.')
  path = args[0]

  SourceDb2()
  # make sure the db is up and we can connect
  CheckDbState(CONNECT_CMD, DISCONNECT_CMD)
 
  main_start_time = time.time()
  dbs_to_keep = DeleteOldBackupFiles(path, BACKUPS_TO_KEEP)

  if len(dbs_to_keep) > 0:
    log_path_root = GetLogPathRoot(SUFFIX_LOG_PATH)
    if os.path.exists(log_path_root):
      current_archive_path = FindRecentLogPath(log_path_root)
    else:
      WriteToSyslog('Bailing. %s does not exist.' % log_path_root, quit=True)
    saved_log_files = DeleteOldLogFiles(dbs_to_keep, log_path_root)
    if options.logverify:
      VerifyLogFiles(current_archive_path, saved_log_files)

  backup_duration, timestamp = BackupDb2(path)

  if timestamp:
    CalculateBackupSize(path, timestamp)
    backup_completed_status = 'Success'
    if options.imageverify:
      image_verified = VerifyImage(path, timestamp)
      if not image_verified:
        backup_completed_status = 'Verify failed'
  else:
    WriteToSyslog('An error occured while running the backup')
    backup_completed_status = 'FAILED'

  RecordDuration(backup_duration, main_start_time)

  WriteToSyslog('The time is now %s' % time.asctime())
  SendEmail(DRAFT_MSG, backup_completed_status)


if __name__ == '__main__':
  main()
