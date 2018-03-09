# -*- coding: UTF-8 -*-
@author Joao J. Vitorino Junior

from pyzabbix import ZabbixAPI
import time
import datetime
import pandas as pd
from timeit import default_timer as timer
import argparse
import getpass


fmt = "%d/%m/%Y"

class Password(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass()

        setattr(namespace, self.dest, values)

parser = argparse.ArgumentParser(description='generates report on xlsx of Zabbix events')
parser.add_argument('--version', action='version', version='2.0')
parser.add_argument('--url', required=True)
parser.add_argument('--login', dest='user', required=True)
parser.add_argument('--pass', action=Password, nargs='?', dest='pwd', required=True)
parser.add_argument('--hosts', default=None, nargs='+')
parser.add_argument('--start', dest='sD', required=True)
parser.add_argument('--end', dest='eD', required=True)
parser.add_argument('--ignore-group', dest='ignoreG', default=['test'], nargs='+')
parser.add_argument('--output', nargs='?', default='zabbix_report.xlsx')
parser.add_argument('--search', nargs='+', default=['Serviço', 'Acesso'])
parser.add_argument('--mtime', default=6, type=int)
args = parser.parse_args()

try:
    startDatetime = datetime.datetime.strptime(args.sD, fmt)
    endDatetime = datetime.datetime.strptime(args.eD, fmt)
except ValueError:
    msg = "Not a valid date. Valid format is dd/mm/yyyy"
    raise argparse.ArgumentTypeError(msg)


time_gap = endDatetime - startDatetime

def convert_time(timestamp='', date=''):
    if timestamp == '':
        return time.mktime(datetime.datetime.strptime(date, fmt).timetuple())
    if date == '':
        return datetime.datetime.fromtimestamp(timestamp)


# Remove from result hosts belonging to ignored groups (default TEST)
def remove_hosts(list_host):
    for ignored in args.ignoreG:
        for l in list_host:
            for g in l['groups']:
                if ignored.lower() in g['name'].lower():
                    print('Host {} in ignored group list. Skipping'.format(l['host']))
                    list_host.remove(l)
    return list_host

# Get list of hosts from zabbix
def get_hosts(listHosts):
    if listHosts is None:
        result = zapi.host.get(selectGroups='extend', smonitored_hosts=True, with_items=True,
                               output=['hostid', 'host', 'groups'])
        return result
    else:
        result = zapi.host.get(selectGroups='extend', smonitored_hosts=True, with_items=True,
                               output=['hostid', 'host', 'groups'], filter={"host": listHosts})
    if not result:
        print('Not host found in list: {}'.format(listHosts))
        exit()
    for l in listHosts:
        if not any(result[x]['host'] == l for x in range(len(result))):
            print('Host {} not found or inactive. Skipping'.format(l))
    result = remove_hosts(result)
    # Delete group information from result list. There is no need anymore
    for h in range(len(result)):
        try:
            del result[h]['groups']
        except KeyError:
            pass
    return result


# Convert string to integer
def to_int(s):
    for index in range(len(s)):
        for key, value in s[index].items():
            try:
                s[index][key] = int(value)
            except (TypeError, ValueError):
                pass
    return s


def get_triggers(host):
    result = zapi.trigger.get(output=['triggerid', 'description'],
                              hostids=host,
                              expandDescription='true',
                              searchByAny=True,
                              search={'description': args.search},
                              )
    if result:
        return to_int(result)
    else:
        return None


def get_event(hostid, triggerid):
    result = zapi.event.get(time_from=startTimeStamp, time_till=endTimeStamp,
                            source='0', hostids=hostid, objectids=triggerid,
                            output=['eventid', 'objectid', 'clock', 'value', 'acknowledged'],
                            select_acknowledges='extend', sortfield=['objectid'], sortorder='ASC')
    if result:
        for i in range(len(result)):
            if result[i]['acknowledged'] == '0':
                del result[i]['acknowledges']
        return to_int(result)
    else:
        return None


def get_message(data, index):
    try:
        m = data[index]['acknowledges'][0]['message']
        u = data[index]['acknowledges'][0]['alias']
    except (KeyError, IndexError):
        m = ''
        u = ''
    return m, u

def percent_time(duration):
    return (duration / time_gap) * 100




def fill_table(table):
    if elapsed_time > datetime.timedelta(minutes=args.mtime):
        table.loc[len(table)] = [hostname, trigger_id, description, start, end, elapsed_time, message, who]

def create_df():
    table = pd.DataFrame(columns=['name', 'triggerid', 'desc', 'start',
                                  'end', 'time', 'ack', 'who'])
    return table

# Open and test connection do Zabbix server
zapi = ZabbixAPI(args.url)
try:
    zapi.login(args.user, args.pwd)
    print('Connected to Zabbix {} at {}'.format(zapi.api_version(), args.url))
except Exception as e:
    print('Fail to connect to Zabbix')
    print('URL: {}'.format(args.url))
    print('Login. {}'.format(args.user))
    print(e)

startTimeStamp = convert_time(date=args.sD)
endTimeStamp = convert_time(date=args.eD)

period = datetime.datetime.strptime(args.eD, fmt) - datetime.datetime.strptime(args.sD, fmt)
print('Period of events: {}'.format(period))
hosts = get_hosts(args.hosts)
tableEvent = create_df()

'''
5 possibilities to get_events
1 - Empty list
2 - List with even number os events and first event status being 1 - Ordinary 
3 - List with odd number os events and first event status being 0 - First event out of range
4 - List with odd number os events and first event status being 1 - Last event out of range
5 - List with even number os events and first event status being 0 - First and last event out of range
'''


for h in hosts:
    triggerHost = get_triggers(h['hostid'])
#    print(triggerHost)
    hostname = h['host']
    if triggerHost is not None:
        for t in triggerHost:
            trigger_id = t['triggerid']
            description = t['description']
            eventsHost = get_event(h['hostid'], t['triggerid'])
            if eventsHost is None:
                pass
            else:
                if len(eventsHost) % 2 != 0 and eventsHost[0]['value'] == 0:
                    for e in range(len(eventsHost)):
                        if e == 0:
                            start = startDatetime
                            end = convert_time(timestamp=eventsHost[e]['clock'])
                            message, who = '', ''
                            elapsed_time = datetime.timedelta(minutes=0)
                        else:
                            if eventsHost[e]['value'] == 1:
                                start = convert_time(timestamp=eventsHost[e]['clock'])
                                try:
                                    end = convert_time(timestamp=eventsHost[e + 1]['clock'])
                                except KeyError:
                                    end = endDatetime
                                message, who = get_message(eventsHost, e)
                                elapsed_time = end - start
                            else:
                                pass
                        fill_table(tableEvent)
                # Case 3 
                elif len(eventsHost) % 2 != 0 and eventsHost[0]['value'] == 1:
                    for e in range(0, len(eventsHost), 1):
                        if e == len(eventsHost):
                            start = convert_time(timestamp=eventsHost[e]['clock'])
                            end = endDatetime
                            message, who = get_message(eventsHost, e)
                        else:
                            if eventsHost[e]['value'] == 1:
                                start = convert_time(timestamp=eventsHost[e]['clock'])
                                try:
                                    end = convert_time(timestamp=eventsHost[e + 1]['clock'])
                                except (KeyError, IndexError):
                                    end = endDatetime
                                message, who = get_message(eventsHost, e)
                                elapsed_time = end - start
                            else:
                                pass
                        fill_table(tableEvent)
                # Case 4 
                elif len(eventsHost) % 2 != 0 and eventsHost[0]['value'] == 1:
                    for e in range(0, len(eventsHost), 1):
                        if e == 0:
                            start = startDatetime
                            end = convert_time(timestamp=eventsHost[e]['clock'])
                            message, who, elapsed_time = '', '', ''
                        elif e == len(eventsHost):
                            start = convert_time(timestamp=eventsHost[e]['clock'])
                            end = endDatetime
                            message, who = get_message(eventsHost, e)
                            elapsed_time = end - start
                        else:
                            if eventsHost[e]['value'] == 1:
                                start = convert_time(timestamp=eventsHost[e]['clock'])
                                try:
                                    end = convert_time(timestamp=eventsHost[e + 1]['clock'])
                                except KeyError:
                                    end = endDatetime
                                message, who = get_message(eventsHost, e)
                                elapsed_time = end - start
                            else:
                                pass
                        fill_table(tableEvent)
                else:  # Case 2
                    for e in range(0, len(eventsHost), 2):
                        if eventsHost[e]['value'] == 1:
                            start = convert_time(timestamp=eventsHost[e]['clock'])
                            try:
                                end = convert_time(timestamp=eventsHost[e + 1]['clock'])
                            except KeyError:
                                end = endDatetime
                            elapsed_time = end - start
                            message, who = get_message(eventsHost, e)
                        fill_table(tableEvent)
    else:  # Case 1
        pass


tablePercent = tableEvent[['name', 'triggerid', 'desc', 'time']].copy()
tablePercent = tablePercent.groupby(['name', 'triggerid', 'desc'])['time'].sum().reset_index()
for row in tablePercent.itertuples():
    tablePercent.set_value(row.Index, 'Percent', float(percent_time(row.time) / 100))

# Add total at end of the table.
tableEvent.loc['Total'] = tableEvent['name'].nunique(), tablePercent['triggerid'].nunique(), \
                          tableEvent['desc'].nunique(), tableEvent['start'].min(), \
                          tableEvent['end'].max(), tableEvent['time'].sum(), 'NaN', 'NaN'

tablePercent.loc['Total'] = tablePercent['name'].nunique(), tablePercent['triggerid'].nunique(), \
                            tablePercent['desc'].nunique(), tablePercent['time'].sum(), \
                            tablePercent['Percent'].sum()


writer = pd.ExcelWriter(args.output, engine='xlsxwriter')
tableEvent.to_excel(writer, sheet_name='acks')
tablePercent.to_excel(writer, sheet_name='Total Time')
workbook = writer.book
# TODO: pass this as args 
workbook.set_properties({
    'author': 'Some name here',
    'company': 'ANS',
    'keywords': 'ZABBIX, report',
    })

sheet1 = writer.sheets['acks']
sheet2 = writer.sheets['Total Time']
format_percent = workbook.add_format({'num_format': '0.00%', 'align': 'right'})
format_left = workbook.add_format({'align': 'left'})
format_timedelta = workbook.add_format({'num_format': 'h:mm:ss'})
# TODO: Multiples formats of dates
format_timedelta2 = workbook.add_format({'num_format': 'd "days" hh:mm:ss'})
sheet1.set_column('A:A', 6, format_left)
sheet1.set_column('B:B', 15, format_left)
sheet1.set_column('D:D', 60, format_left)
sheet1.set_column('E:F', 20, format_left)
sheet1.set_column('G:G', 10, format_timedelta2)
sheet1.set_column('H:H', 30, format_left)
sheet1.set_column('I:I', 15, format_left)
sheet2.set_column('A:A', 6, format_left)
sheet2.set_column('B:B', 15, format_left)
sheet2.set_column('D:D', 60, format_left)
sheet2.set_column('E:E', 10, format_timedelta2)
sheet2.set_column('F:F', 10, format_percent)
writer.save()

exit(0)
