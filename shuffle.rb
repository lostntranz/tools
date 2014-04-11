#!/usr/bin/env ruby
#
# Evacuate from a lagging db to reduce load
# so that it can catches back up with master
#
# kludgy fix until better tooling is available

require 'optparse'
require 'chef/knife'
require 'net/ssh'
require 'net/ssh/multi'
require 'highline/import'
require 'json'

def knife
  return @knife if @knife
  chef_config_file = File.join(ENV['HOME'], '.chef', 'knife.rb')
  @knife = Chef::Knife.new
  @knife.config[:config_file] = chef_config_file
  @knife.configure_chef
  @knife
end

def check_role(input)
  base_dir = File.dirname(__FILE__)
  if input[0..2].eql? 'ip-'
    role_file = File.join(base_dir, '../roles', "#{hostname_to_role(input)}.rb")
  else
    role_file = File.join(base_dir, '../roles', "#{input}.rb")
  end
  if !File.exist? role_file
    STDERR.puts "Unable to find '#{role_file}'"
    STDERR.puts "#{input} does not appear to have proper associated role.\nPlease check the number and dial again\n"
    exit(1)
  end
end

def hostname_to_role(fqdn)
  hostname, rack, dc, domain = fqdn.split('.')
  hostname.gsub!('ip-', '')
  role = "tc5-use-db-slave-#{hostname}"
  return role
end

def get_api_hosts(source_db, quantity)
  role = hostname_to_role(source_db)
  nodes = JSON.parse(%x(knife search node -F json "role:#{role}" -i))["rows"]
  quantity ? nodes.last(quantity) : nodes
end

def get_old_role(fqdn)
  role_prefix = "tc5-use-db"
  old_role = %x(knife node show #{fqdn} -r |grep #{role_prefix}).strip
  return old_role
end

def restart_thins(client_list)
  thread_pool = []
  for host in client_list
    thread_pool << Thread.new(host) { |remote_host|
    puts "Exec: Restarting thins on #{remote_host}"
    Net::SSH::Multi.start do |ssh|
      ssh.use remote_host, :user => ENV['USER']
      ssh.exec 'for x in `ls /etc/sv |egrep "(^soundcloud-)|(notifications-)" |grep -v restart`; do echo "Restarting $x"; sudo su -l -c "sv -w 60 restart $x"; done'
    end
    }
  end
  thread_pool.each { |thread| thread.join }
end

def check_running_chef(host)
  until  %x(ssh #{host} "ps axuww |grep 'bin/chef-client' | grep -v grep").empty?
    puts "Chef appears to be running on the #{host}, waiting.."
    sleep 20
  end
end

option = {}
optparser = OptionParser.new do |opt|
  opt.banner = "
Usage: shuffle.rb [ -c client(s) | -s source_db (fqdn)| -f path_to_file ] -t target db (fqdn)
Example: ./shuffle.rb -c ip-10-33-41-28.m11.ams5.s-cloud.net,ip-10-33-19-38.n04.ams5.s-cloud.net -t ip-10-33-19-51.n04.ams5.s-cloud.net
\t./shuffle.rb -n4 -s ip-10-33-41-42.m11.ams5.s-cloud.net -t ip-10-33-19-51.n04.ams5.s-cloud.net
\t./shuffle.rb -s ip-10-33-41-42.m11.ams5.s-cloud.net -t lb-soundcloud ## evacuate all clients to slave pool.\n\n"
  opt.on("-c", "--client CLIENT", "Map a single client or multiple clients separated by comma. White space is allowed but will need to be quoted") do |c|
    option[:client] = c
  end
  opt.on("-s", "--source DB", "Evacuate from this source db. Makes no assumption on specific hosts to move, rather its by the number. See -n option. ") do |sd|
    check_role(sd)
    option[:source] = sd
  end
  opt.on("-n", "--number ", Integer, "Number of app server. Default is all. Use in conjunction with -s option") do |n|
    option[:num] = n
  end
  opt.on("-f", "--file FILENAME", "Use hosts listed in this file. Mainly for pinning a list of hosts to new DB server.  ") do |fh|
    option[:file] = fh
  end
  opt.on("-t", "--target DB", "Move to this target db, or use 'lb-soundcloud' to move to slave pool") do |td|
    check_role(td)
    option[:target] = td
  end
  opt.on("-h", "--help", "Print this screen") do
    puts opt
    exit
  end
end

optparser.parse!(ARGV)
input = [option[:source], option[:file], option[:client]]
if input.all? {|x| x.nil?} || option[:target].nil?
  puts "Need to specify either a client [-c], a source db [-s] or file [-f] option and a target [-t]"
  STDERR.puts optparser.help()
  exit(1)
end

client_list = []
if option[:file]
  client_list = File.readlines(option[:file]).collect! {|line| line.chomp! }
  client_list.reject! {|field| field.empty? }
elsif option[:source]
  client_list = get_api_hosts(option[:source], option[:num])
else
  client_list = option[:client].split(/[\s,]+/)
end

client_list.each { |x| puts "Moving #{x} to use #{option[:target]}?" }
puts
ans = agree("Does the above action looks right to you? (y/n) ") { |a| a.default = "y" }
exit(0) if !ans

client_list.each do |host|
  check_running_chef(host)
  puts "slog: moving #{host} to use db slave #{option[:target]}"
  if option[:source]
    old_role = "role[#{hostname_to_role(option[:source])}]"
  else
    old_role = get_old_role(host)
  end
  puts "Exec: Removing roles #{old_role} from #{host}"
  %x(knife node run_list remove #{host} "#{old_role}")
  if option[:target][0..2].eql? 'ip-'
    new_role = "role[#{my_role(option[:target])[2]}]"
  else
    new_role = "role[#{option[:target]}]"
  end
  # 
  if not option[:target].eql? 'lb-soundcloud'
    puts "Exec: Applying roles #{new_role} to #{host}"
    %x(knife node run_list add #{host} "#{new_role}")
  end
  puts "Exec: Running kchef on #{host}"
  %x(ssh -oStrictHostKeyChecking=no #{host} 'sudo kchef')
  #
end

restart_thins(client_list)
