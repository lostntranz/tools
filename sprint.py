#!/usr/bin/env python

####
# sprint.py - Multi-threaded command trigger. 
# Initiating many threads in parallel on remote
# hosts. Roles are implementation of chef role 
# or passing a list of hosts as input file.
#
# $Author: daman $
# $Date: 2013/05/01
# $Name: sprint.py
# $State: Exp $

####
import os
import sys
import glob
import time
import string
import commands
import subprocess

from threading import Thread
from optparse import OptionParser

DEBUG = 0

WORK_DIR = os.path.dirname(os.path.realpath(__file__))
CACHE_DIR = "/tmp/list_cache"
CHEF_ENV = "%s/.chef/knife.rb" % os.environ['HOME']
CACHE_TIME = 86400
SSH_FLAG = "-oStrictHostKeyChecking=no"

class RunCmd(Thread):
  def __init__ (self,host):
    Thread.__init__(self)
    self.host = host
    self.status = 'Unable to process'
  def run(self):
    # keep trying until all children return with something
    while 1:
      try:
        cmd = subprocess.Popen(['ssh', SSH_FLAG, self.host, options.command ],
        # grabbing all ssh stdin/stderr
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, close_fds=True)
        while 1:
          if cmd:
            # waiting for kids to return
            output = cmd.communicate()[0]
            self.status = output.strip()
            break
      except OSError:
        #print '%s caught error- suppressing them' % self.host
        sys.stderr.flush()
        # handle tracebacks
        err = open('/dev/null', 'a+', 0)
        # suppressing some noise
        os.dup2(err.fileno(), sys.stderr.fileno())
        pass
      else:
        break


def run_now(list_of_hosts):
  alist = []
  for host in list_of_hosts:
    current = RunCmd(host)
    alist.append(current)
    current.start()
  if options.verbose:    # helping the '-v' flag
    print 'DATE: %s' % commands.getoutput('date')
    print 'HOSTS: %s\n' % ' '.join(list_of_hosts)
  for a in alist:
    a.join()

    if options.verbose:
      print '%s:\n %s\n' % (a.host, a.status)
    else:
      print '%s: %s' % (a.host, a.status)

def mkbatch(host_list, rate):
      return [host_list[i:i+rate] for i in range(0, len(host_list), rate)]

def run_it(host_list):
  start_time = time.time()
  run_now(host_list)
  stop_time = time.time()
  time_elapse = float(stop_time) - float(start_time)
  print 'processed time: %.2f' %  time_elapse

def get_host_list(group_list, host_file):
  for group in group_list:
    for lines in open(host_file, 'r'):
      if group in lines:
        host_group = lines.split(':')[1]
        host_list = [ host.strip() for host in host_group.split(',') ]
  return host_list

def get_groups(verbose):
  host_files = []
  for files in glob.glob("%s/*.txt" % CACHE_DIR):
    rolename, ext = os.path.splitext(os.path.basename(files))
    host_files.append(rolename)
  host_files.sort()
  for role in host_files:
    print role

def gen_host_file(value, host_file):
  output = subprocess.Popen(['knife', 'search', 'node', "roles:%s" % value, '-c', '%s' % CHEF_ENV, '-i'], shell=False, stdout=subprocess.PIPE )
  return_hosts = [ lines.strip() for lines in output.stdout.readlines() if lines.strip() ]
  if len(return_hosts) == 1:
    print "No host found under role:%s" % value
    sys.exit(1)
  else:
    return_hosts.pop(0)
    f = open(host_file, 'w')
    f.write("%s: %s\n" % (value, ','.join(return_hosts)))
    f.close

def initiate_process(role_list):
  host_list = []
  for each_role in role_list:
    host_file = "%s/%s.txt" % (CACHE_DIR, each_role)
    try:
      file_time = os.path.getmtime(host_file)
      if ((time.time() - file_time) > CACHE_TIME):
        # Over a day old, regenerate fresh host file"
        print "Refreshing cache file for %s" % each_role
        gen_host_file(each_role, host_file)
    except OSError:
    # Create a new role file here
      print "No cache file found for %s, generating.." % each_role
      gen_host_file(each_role, host_file)
    for host in get_host_list(each_role, host_file):
      host_list.append(host)
  return host_list

def main():
  global options
  # define usage
  Usage = """%prog [-rivl] -c 'command' OPTIONAL: [ov] 

Example:
    sprint.py -l                                                     # list cached groups
    sprint.py -r web-app -c 'uptime'
    sprint.py -r web-lb mail dns -c 'date'                     # supports multiple roles 
    sprint.py -r mysql -c 'mysql -e "show slave status\G"'   # even supports '*' wildcard
    sprint.py -i /tmp/list_of_host -c 'free -m'                       # reads list of host from file
"""
  Description = """Host list files are only cached for 1 day."""

  parser = OptionParser(Usage,  description=Description)
  parser.add_option("-l", "--list", dest="list", action="store_true", help="List all previously queried (cached) roles")
  parser.add_option("-r", "--role", action="store", dest="role", help="Perform action or list hosts under this role or roles")
  parser.add_option("-i", "--infile", action="store", dest="infile", help="Process hosts as listed in this file")
  parser.add_option("-c", "--command", action="store", dest="command", help="Run this command")
  parser.add_option("-b", "--batch", action="store", dest="batch", help="Break up hosts in batches of n.")
  parser.add_option("-w", "--wait", action="store", dest="wait", help="Use with -b: This specified the time wait in between batches")
  parser.add_option("-v", "--verbose", help="Show date and group header", action="store_true")
  parser.add_option("-o", "--outfile", action="store", dest="outfile", help="Write stdout to a file")

  options, args = parser.parse_args()
  # what to do with all them options
  if  len(sys.argv) == 1:
    parser.print_help()
    sys.exit(0)

  if not options.role and not options.infile and not options.list:
    parser.error('Please include a role using -r (or and infile using -i $filename) when using -c "command"')

  if options.list:
    get_groups(options.verbose)
  else:
    if options.role:
      role_list = [options.role]
      [ role_list.append(i) for i in args if args ]
      host_list = initiate_process(role_list)
    elif options.infile:
      print "FILE: %s" % options.infile
      host_list = open(options.infile, 'r').read().split()
    batch_list = []
    if options.command:
      # finally, here we go
      if options.outfile:
        sys.stdout = open(options.outfile, 'w')
      if options.batch:
	if not options.wait:
	  parser.error('Must include a wait time -w # when using -b option in batch mode')
        batch_list.append(mkbatch(host_list, int(options.batch)))
        for batches in batch_list:
	  for hosts in batches:
	    run_it(hosts)
	    time.sleep(float(options.wait))
      else:
	# do all at once
	run_it(host_list)
    else:
      for host in host_list:
        print "%s" % host

if __name__ == "__main__":
  if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
  main()
