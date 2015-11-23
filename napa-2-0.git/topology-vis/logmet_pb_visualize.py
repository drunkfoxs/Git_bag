#!/usr/bin/python

import sys
import string
from exceptions import Exception

from collections import namedtuple, OrderedDict, defaultdict
import re
import traceback
import math
import subprocess
import json
import bottle
from bottle import route, static_file, request

import ConfigParser
import csv
import datetime
from datetime import date, timedelta
import os.path
import requests
import thread
import threading
import time


#Read Configuration File
CONFIG_FILE = '/Git_bag/napa-2-0.git/topology-vis/monitors.conf'
config_data = ConfigParser.RawConfigParser(allow_no_value=True)
tokens = {}

# Calculates time delta in miliseconds - This function requires as input datetime objects
def calculate_time_delta_miliseconds(old_time, new_time):
    delta = new_time - old_time
    milis = int(delta.total_seconds() * 1000)
    return milis


# Calculates time delta in minutes - Input are two strings representing dates (ie. 20150727185001)
def calculate_time_delta_minutes(old_time, new_time):   
    old_timestamp = time.strptime(old_time, '%Y%m%d%H%M%S')
    new_timestamp = time.strptime(new_time, '%Y%m%d%H%M%S')
    old = time.mktime(old_timestamp)
    new = time.mktime(new_timestamp)
    time_delta = new - old
    minutes = int(time_delta) / 60
    return minutes
    
    
# Calculates time delta in seconds
def calculate_time_delta_seconds(old_time,new_time):
    old_timestamp = time.strptime(old_time, '%Y%m%d%H%M%S')
    new_timestamp = time.strptime(new_time, '%Y%m%d%H%M%S')
    old = time.mktime(old_timestamp)
    new = time.mktime(new_timestamp)
    time_delta = new - old
    return int(time_delta)

def get_auth_tokens():
    #Get a new timestamp
    # timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # #Retrieve old timestamp from config file
    # old_timestamp = config_data.get('tokens', 'token_creation_time')
    # #Retrieve token expiration time in minutes
    # expiration_time_minutes = config_data.get('tokens', 'token_expiration_interval_min')
    # #Calculate the delta of the token timestamp and the current timestamp
    # token_time_delta = calculate_time_delta_minutes(old_timestamp,timestamp)
    # if (int(token_time_delta) < int(expiration_time_minutes)): 
    # #    print "Valid token found. No authentication required."
    #     tokens_tmp = {'logging_token': config_data.get('tokens', 'logging_token'),'access_token': config_data.get('tokens', 'access_token'), 'space_id': config_data.get('tokens', 'space_id')}
    #     return tokens_tmp
    
    server = "logmet.ng.bluemix.net"
    #print "Requesting authentication token from server %s" % (server)

    user = "ylyang@cn.ibm.com"
    passwd = "passw0rd"
    space = "devel"
    organization = "shriram@us.ibm.com"
    
    data={'user': user, 'passwd': passwd, 'space': space, 'organization': organization}
    
    try :
        login_request = requests.post("https://" + server + "/login", data=data)
        login_status = login_request.status_code
    except:
        login_status = 401
        e = sys.exc_info()[0]
        v = sys.exc_info()[1]
        raise Exception("Exception while requesting a token: %s %s" % (str(e),str(v)))
    
    if (login_status == 200):
        tokens_json = login_request.json()
        tokens_tmp = {'logging_token': tokens_json['logging_token'],'access_token': tokens_json['access_token'], 'space_id':tokens_json['space_id']}
        # if not config_data.has_section("tokens"):
        #     config_data.add_section("tokens")
        # config_data.set('tokens','logging_token',tokens_tmp['logging_token'])
        # config_data.set('tokens','access_token',tokens_tmp['access_token'])
        # config_data.set('tokens','space_id',tokens_tmp['space_id'])
        # config_data.set('tokens','token_creation_time',timestamp)
        # write_config()
        #print "Tokens retrieved successfully."
    else:
        tokens_tmp = None
        raise Exception("Errors while retrieving tokens. HTTP code %d" % (login_status))
        
    return tokens_tmp

def write_config():

    with open(CONFIG_FILE, 'wb') as configfile:
        config_data.write(configfile)
    

def query_elastic_search(date_tags, start_ts, tokens = None):
    log_status = 400

    if not tokens:
        tokens = get_auth_tokens()
    #Prepare headers
    request_headers = {'X-Auth-Token':tokens['access_token'], 'X-Auth-Project-Id':tokens['space_id']}
    
    #Prepare url
    server = "logmet.ng.bluemix.net"
    log_names=[]
    for d in date_tags:
        log_names.append("logstash-" + tokens['space_id'] + "-" + d)
    log_name = ",".join(log_names)
    resource_path = "/elasticsearch/" + log_name + "/_search"
    search_url = "https://" + server + resource_path
    
    print search_url
    
    #Prepare data statement
    request_data = json.dumps({
      "query": {
        "filtered": {
          "query": {
            "bool": {
              "should": [
                {
                  "query_string": {
                    "query": "*"
                  }
                }
              ]
            }
          },
          "filter": {
            "bool": {
              "must": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": start_ts #1445032800000
                      
                    }
                  }
                }
              ]
            }
          }
        }
      },
      "size": 500,
      "sort": [
        {
          "@timestamp": {
            "order": "desc",
            "ignore_unmapped": True
          }
        },
        {
          "@timestamp": {
            "order": "desc",
            "ignore_unmapped": True
          }
        }
      ]
    })
    
    print request_data
    print request_headers
    
    query_response = requests.post(search_url, headers=request_headers, data=request_data, stream=True)
    log_status = query_response.status_code
    if (log_status == 200) :
        json_response = json.loads(query_response.text)
        if json_response['hits']['total'] == 0 :
            raise Exception("No entries from elasticsearch!")
        entries = []
        for e in json_response['hits']['hits']:
            entries.append(e['_source']['log'])
        return entries

    raise Exception("Query returned status %d" % (int(log_status)))


mapping = json.load(open('mappings.json'))
graphnodes = {}
graphedges = []
LATENCY_THRESHOLD=10.0
    # var nodes = %s;
    # //[
    # // {id: 1, label: 'Node 1'},
    # // {id: 2, label: 'Node 2'},
    # // {id: 3, label: 'Node 3'},
    # // {id: 4, label: 'Node 4'},
    # // {id: 5, label: 'Node 5'}
    # // ];

    # // create an array with edges
    # var edges = %s;
    # //[
    # // {from: 1, to: 2},
    # // {from: 1, to: 3},
    # // {from: 2, to: 4},
    # // {from: 2, to: 5}
    # // ];

level_dict = {
    "External" : 0,
    "zookeeper" : 1,
    "auth" : 1,
    "view" : 1,
    "serviceA" : 2,
    "serviceB" : 2,
    "serviceC" : 2,
    "serviceD" : 2,
    "serviceA1" : 3,
    "serviceA2" : 3,
    "serviceA3" : 3,
    "serviceB1" : 3,
    "serviceB2" : 3,
    "serviceB3" : 3,
    "serviceC1" : 3,
    "serviceC2" : 3,
    "serviceC3" : 3,
    "serviceD1" : 3,
    "serviceD2" : 3,
    "serviceD3" : 3,
    "serviceDB1" : 4,
    "serviceDB2" : 4,
    "serviceDB3" : 4,
    "serviceAsync1" : 5,
    "serviceAsync2" : 5,
    "serviceAsync3" : 5
}

def getLabel(ip):
    if mapping is None:
        return ip
    if ip not in mapping:
        label="External"
    else:
        label=mapping[ip]
#    print 'returning %s:%s' % (ip, label)
    return label

id_counter = 0
def addNode(label, ips = None):
    # if label in graphnodes:
    #     return
    global id_counter
    id_counter += 1
    graphnodes[label] = { 'id': id_counter, 'label': label}
#    print 'adding mapping: id %d, label %s' % (len(graphnodes)+1, label)
    if label in level_dict:
        graphnodes[label]['level']= level_dict[label]
    if ips:
        graphnodes[label]['title'] = ', '.join(str(ip) for ip in ips)

def addEdge(f, t, v):
    e = { 'from': f, 'to': t, 'value': v, 'title': "%s ms" % ("<1" if (v < 1) else str(v))}
    if v > LATENCY_THRESHOLD:
        e['color']='red'
    graphedges.append(e)
    return e

def create_nodes_edges(edge_dict, service_instances):
    for label, ips in service_instances.iteritems():
        addNode(label, ips)

    for c, tstat in edge_dict.iteritems():  
        e = addEdge(graphnodes[c.src]['id'], graphnodes[c.dst]['id'], int(tstat.get_req_stat()))
        #e['label'] = str(tstat)


class Endpoint(namedtuple('Endpoint', ['ip','port'])):
    __slots__ = ()
    def __str__(self):
        if self.port:
            return '%s.%d' % (self.ip, self.port)
        else:
            return '%s' % (self.ip)

class Connection:

    def __init__(self, p1, p2):

        self.p1 = p1
        self.p2 = p2

    def __cmp__(self, other):
        if ((self.p1 == other.p1 and self.p2 == other.p2)
            or (self.p1 == other.p2 and self.p2 == other.p1)):
            return 0
        else:
            return -1

    def __hash__(self):
        return (hash(self.p1[0]) ^ hash(self.p1[1])
                ^ hash(self.p2[0]) ^ hash(self.p2[1]))

    def __str__(self):
        return '%s.%d-->%s.%d' %(self.p1[0],self.p1[1],self.p2[0],self.p2[1])

class Edge:

    def __init__(self, src, dst):
        self.src = getLabel(src)
        self.dst = getLabel(dst)

    def __str__(self):
        return '%s-->%s' %(self.src,self.dst)

    def __cmp__(self, other):
        if (((self.src == other.src) and (self.dst == other.dst)) or
            ((self.src == other.dst) and (self.dst == other.src))):
            return 0
        else:
            return -1

    def __hash__(self):
        return (hash(self.src) ^ hash(self.dst))

average = lambda x: sum(x) * 1.0 / len(x)
variance = lambda x: map(lambda y: (y - average(x)) ** 2, x)
stddev = lambda x: math.sqrt(average(variance(x)))

## http://code.activestate.com/recipes/511478/
def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return N[int(k)]
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

class LatencyStats(namedtuple('LatencyStats', ['name','lmin','lmax','lmedian','lpercentile'])):
    __slots__ = ()
    
    def __str__(self):
        return '%s: min=%.2fms, max=%.2fms, med=%.2fms, 95th percentile=%.2fms' % (self.name, self.lmin,
                                                                                   self.lmax, self.lmedian,
                                                                                   self.lpercentile)

class TimingStats:
    def __init__(self):
        self.reqs = []
        
    def addStat(self, req):
        self.reqs.append(req)

    def calcTrends(self):      
        self.preqs = sorted(self.reqs)
        self.request_stats = 0.0;
        if (max(self.preqs) > 0):
            self.request_stats = LatencyStats(name='req_latency',lmin=min(self.preqs), lmax=max(self.preqs),
                                              lmedian = percentile(self.preqs, percent=0.5),
                                              lpercentile = percentile(self.preqs, percent=0.95))
        else:
            self.request_stats = LatencyStats(name='req_latency',lmin=0.0,lmax=0.0,lmedian=0.0,lpercentile=0.0)

  
    def get_req_stat(self):
        return self.request_stats.lpercentile

    def __str__(self):
        return '%.2f' % (self.request_stats.lpercentile)

# 
# {"packetbeat":
#     {
#         "bytes_in":204,"bytes_out":203,"client_ip":"172.31.0.27","client_port":60486,"client_proc":"","client_server":"","count":1,"direction":"out",
#         "http":{"code":200,"content_length":50,"phrase":"OK"},
#         "ip":"172.31.0.24","method":"GET","params":"","path":"/bottle/all/view/serviceA/serviceA1","port":9080,
#         "proc":"","query":"GET /bottle/all/view/serviceA/serviceA1",
#         "responsetime":0,"server":"","shipper":"instance-0004864f","status":"OK",
#         "timestamp":"2015-10-16T22:38:36.383Z","type":"http"
#         }
# }

def do_analyze(pbrecords):
    # Open file
    edges = dict()
    connections = dict()
    service_instances = defaultdict(set)
    for t in pbrecords:
        if not len(t):
            continue
 
        t1 = json.loads(str(t).strip('\r\t\n\0'))
        p = t1['packetbeat']
        src = (p['client_ip'], p['client_port'])
        dst = (p['ip'], p['port'])
        respTime = int(p['responsetime'])
        c = Connection(src,dst)
        if (c not in connections) or (connections[c] < respTime):
            connections[c] = respTime

    for c in connections:
        l1 = getLabel(c.p1[0])
        service_instances[l1].add(c.p1[0])
        l2 = getLabel(c.p2[0])
        service_instances[l2].add(c.p2[0])
        a = Edge(c.p1[0],c.p2[0])
        if a not in edges:
            tstat = TimingStats()
            edges[a] = tstat
        else:
            tstat = edges[a]
        tstat.addStat(connections[c])
    for e in edges:
        tstat = edges[e]
        tstat.calcTrends()
    create_nodes_edges(edges, service_instances)

#@route("/analyze", method = "GET")
@route("/analyze", method = "POST")
def analyze():
    user = request.forms.get('user')
    passwd = request.forms.get('passwd')
    space = request.forms.get('space')
    organization = request.forms.get('organization')
    if '' in [user, passwd, space, organization]:
        return 'One of the fields is empty.'
    config_data.read(CONFIG_FILE)
    config_data.set('auth','user', user)
    config_data.set('auth','passwd', passwd)
    config_data.set('auth','space', space)
    config_data.set('auth','organization', organization)
    tokens = get_auth_tokens()
    # d = request.query['start_time']
    # if not d:
    #     return 'Invalid date!'
    # start_time = datetime.datetime.utcfromtimestamp(time.mktime(time.strptime(d, '%Y-%m-%dT%H:%M')))
    # date_tag = ['2015.10.16','2015.10.17','2015.10.18','2015.10.19','2015.10.20']
    # start_ts = int(start_time.strftime('%s')) * 1000
    # #1445032800000 datetime.datetime.utcfromtimestamp(1445311620).strftime('%s')
    # print 'date tag is %s, time is %s' % (date_tag, str(start_ts))
    pbrecords = query_elastic_search(['2015.10.20'], 1445227571107, tokens)

    do_analyze(pbrecords)

    content = """
    <!doctype html>
    <html>
    <head>
    <title>Iodine - Application Topology & Request Latencies</title>

    <script type="text/javascript" src="http://visjs.org/dist/vis.js"></script>
    <link href="http://visjs.org/dist/vis.css" rel="stylesheet" type="text/css">
    </head>

    <body>

    <div id="mynetwork"></div>

    <script type="text/javascript">

    var nodes = %s;
    var edges = %s;
    var container = document.getElementById('mynetwork');
    var data= {
       nodes: nodes,
       edges: edges,
    };
    var options = {
        width: '50%%',
        height: '600px',
        configure: {
            enabled:false,
            filter:true,
            showButton:true
        },
        edges: {
           color: 'green'
        },
        nodes: {
            color: 'gray'
        },
        layout: {
            hierarchical: {
                enabled:true,
                levelSeparation: 100,
                direction: "UD",
                sortMethod: "directed"
            }
        }
     };
    var network = new vis.Network(container, data, options);

    </script>
    </body>
    </html>
    """ % (json.dumps(graphnodes.values()), json.dumps(graphedges))

    return content

@route("/")
def landing():
    return static_file('index.html', root='.')

# @route('/login', method = 'POST')
# def login():
#     user = request.forms.get('user')
#     passwd = request.forms.get('passwd')
#     space = request.forms.get('space')
#     organization = request.forms.get('organization')
#     if '' in [user, passwd, space, organization]:
#         return 'One of the fields is empty.'
#     config_data.read(CONFIG_FILE)
#     config_data.set('auth','user', user)
#     config_data.set('auth','passwd', passwd)
#     config_data.set('auth','space', space)
#     config_data.set('auth','organization', organization)
#     tokens = get_auth_tokens()
#     return static_file('analyze.html', root='.')

if __name__ == '__main__':
    tokens = get_auth_tokens()
    print tokens
    print query_elastic_search(['2015.11.01'],1446350066000 , tokens)
    
    # 1445227571107