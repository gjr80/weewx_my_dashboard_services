"""
mqtt_dashboard.py

A few WeeWX services for publishing to a MQTT broker.

Portions based on the MQTT uploader v0.23 Copyright 2013-2021 Matthew Wall and
distributed under the terms of the GNU Public License (GPLv3).

Copyright (C) 2021 Gary Roderick                gjroderick<at>gmail.com

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see http://www.gnu.org/licenses/.

Version: 0.1.0                                        Date: 9 March 2021

Revision History
    9 March 2021        v0.1.0
        -   initial release

This file contains the class definitions for the following WeeWX services:

MQTTDashboardLoop - a RESTful service for publishing WeeWX loop data to a MQTT
                    broker

MqttDashboardAeris - a service for obtaining and publishing Aeris forecast and
                     conditions data to a MQTT broker

MqttDashboardSlow - a service for publishing slow changing observation (usually
                    aggregate) data to a MQTT broker every archive period

Supporting class definitions include:

MqttPublisher - a class for publishing data to a MQTT broker

TLSDefaults - a class used to construct default TLS options for use when using
              TLS to conenct to a MQTT broker

DayCache - a specialist cache of day based observation data

"""
# Python imports
import configobj
import datetime
import json
import operator
import paho.mqtt.client as mqtt
import random
import socket
import sys
import threading
import time

# Python2/3 imports in different libraries
try:
    import queue as Queue
except ImportError:
    import Queue

try:
    from urllib.error import URLError
    from urllib.parse import quote, urlparse, urlencode, parse_qs
    from urllib.request import urlopen
except ImportError:
    from urllib import quote, urlencode
    from urllib2 import urlopen, URLError
    from urlparse import urlparse, parse_qs

# WeeWX imports
import weeutil
import weeutil.Moon
import weewx
import weewx.almanac
import weewx.engine
import weewx.manager
import weewx.restx
import weewx.tags
from weeutil.config import conditional_merge
from weewx.units import ValueTuple
from weeutil.weeutil import to_bool, to_float, to_int, timestamp_to_string, accumulateLeaves

# import/setup logging, WeeWX v3 is syslog based but WeeWX v4 is logging based,
# try v4 logging and if it fails use v3 logging
try:
    # WeeWX4 logging
    import logging
    from weeutil.logger import log_traceback

    log = logging.getLogger(__name__)

    def logdbg(msg):
        log.debug(msg)

    def loginf(msg):
        log.info(msg)

    def logcrit(msg):
        log.critical(msg)

    def logerr(msg):
        log.error(msg)

    # log_traceback() generates the same output but the signature and code is
    # different between v3 and v4. We only need log_traceback at the log.error
    # level so define a suitable wrapper function.
    def log_traceback_critical(prefix=''):
        log_traceback(log.critical, prefix=prefix)

    def log_traceback_error(prefix=''):
        log_traceback(log.error, prefix=prefix)

except ImportError:
    # WeeWX legacy (v3) logging via syslog
    import syslog
    from weeutil.weeutil import log_traceback

    def logmsg(level, msg):
        syslog.syslog(level, 'mqtt_dashboard: %s' % msg)

    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)

    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)

    def logcrit(msg):
        logmsg(syslog.LOG_CRIT, msg)

    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

    # log_traceback() generates the same output but the signature and code is
    # different between v3 and v4. We only need log_traceback at the log.error
    # level so define a suitable wrapper function.
    def log_traceback_critical(prefix=''):
        log_traceback(prefix=prefix, loglevel=syslog.LOG_CRIT)

    def log_traceback_error(prefix=''):
        log_traceback(prefix=prefix, loglevel=syslog.LOG_ERR)


# ============================================================================
#                     Exceptions that could get thrown
# ============================================================================

class MissingApiKey(IOError):
    """Raised when an API key cannot be found"""


class UnknownServer(IOError):
    """Raised when an invalid or missing server URL is provided"""


class FailedPost(IOError):
    """Raised when a post fails after trying the max number of allowed times"""


# ============================================================================
#                            class MQTTDashboard
# ============================================================================

class MQTTDashboard(weewx.restx.StdRESTbase):
    """Class to publish MQTT messages to a broker for use by the bootstrap dashboard.

        The basic process is to:
        1.  convert incoming loop packets to METRIC,
        2.  update the day cache with the converted packet,
        3.  extract from the day cache a loop packet with max-min data,
        4.  pass the max-min packet to MQTTDashboard thread via a queue
        5.  the MQTTDashboard thread publishes the max-min packet

        This service recognizes standard restful options plus the following:

        Required parameters:

        server_url: URL of the broker, e.g., something of the form
          mqtt://username:password@localhost:1883/
        Default is None

        Optional parameters:

        unit_system: one of US, METRIC, or METRICWX
        Default is None; units will be those of data in the database

        topic: the MQTT topic under which to post
        Default is 'weather'

        obs_to_upload: Which observations to upload.  Possible values are
        none or all.  When none is specified, only items in the inputs list
        will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is all

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None

        tls: dictionary of TLS parameters used by the Paho client to establish
        a secure connection with the broker.
        Default is None
    """
    version = '0.1.0'

    def __init__(self, engine, config_dict):

        # initialise our base class
        super(MQTTDashboard, self).__init__(engine, config_dict)

        loginf("Initializing version %s" % MQTTDashboard.version)
        # obtain our site config dict, fail hard if we are missing any critical
        # config entries
        try:
            site_dict = config_dict['StdRESTful']['MQTTDashboard']
            site_dict = accumulateLeaves(site_dict, max_level=1)
            site_dict['server_url']
        except KeyError as e:
            logerr("Data will not be uploaded: Missing option %s" % e)
            return

        # set defaults for any missing optional config entries
        site_dict.setdefault('client_id', '')
        site_dict.setdefault('topic', 'weather')
        site_dict.setdefault('augment_record', True)
        site_dict.setdefault('obs_to_upload', 'all')
        site_dict.setdefault('retain', False)
        site_dict.setdefault('qos', 0)
        site_dict.setdefault('unit_system', 'METRIC')

        if 'tls' in config_dict['StdRESTful']['MQTTDashboard']:
            site_dict['tls'] = dict(config_dict['StdRESTful']['MQTTDashboard']['tls'])

        if 'inputs' in config_dict['StdRESTful']['MQTTDashboard']:
            site_dict['inputs'] = dict(config_dict['StdRESTful']['MQTTDashboard']['inputs'])

        site_dict['augment_record'] = to_bool(site_dict.get('augment_record'))
        site_dict['retain'] = to_bool(site_dict.get('retain'))
        site_dict['qos'] = to_int(site_dict.get('qos'))
        binding = site_dict.pop('binding', 'archive')

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        try:
            if site_dict.get('augment_record'):
                _manager_dict = weewx.manager.get_manager_dict_from_config(
                    config_dict, 'wx_binding')
                site_dict['manager_dict'] = _manager_dict
        except weewx.UnknownBinding:
            pass

        # obtain the unit system to use for uploads, default to METRIC
        self.unit_system = weewx.units.unit_constants.get(site_dict.pop('unit_system', 'METRIC').upper(),
                                                          weewx.METRIC)

        # create a Queue object for our thread and then start the thread to do
        # the publishing
        self.loop_queue = Queue.Queue()
        self.loop_thread = MqttDashboardThread(self.loop_queue, **site_dict)
        self.loop_thread.start()

        # obtain a DayCache object to keep track of day max/min etc
        self.day_cache = DayCache()

        # bind ourselves to the NEW_LOOP_PACKET event
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        # now log our config
        loginf("Binding to %s" % binding)
        loginf("Data will be uploaded to %s" %
               obfuscate_password(site_dict['server_url']))
        if 'topic' in site_dict:
            topic_msg = "Topic is %s," % site_dict['topic']
        else:
            topic_msg = ''
        loginf("%s qos is %d, retain is %r." % (topic_msg,
                                                site_dict['qos'],
                                                site_dict['retain']))
        if site_dict['obs_to_upload'].lower() == 'all':
            loginf("All fields will be uploaded")
        elif 'inputs' in site_dict:
            inputs_str = ', '.join(sorted(site_dict['inputs'].keys(), key=str.lower))
            loginf("The following fields will be uploaded: %s" % inputs_str)
        loginf("Data will be uploaded using the %s unit "
               "system" % weewx.units.unit_nicknames.get(self.unit_system))
        if 'tls' in site_dict:
            loginf("Network encryption/authentication will be attempted")

    def new_loop_packet(self, event):
        """Process a new loop packet."""

        # first convert the packet to our unit system, we need to do this
        # before we add the packet to the day cache because the day cache
        # provides a packet that includes max-min data preventing us from using
        # WeeWX converters to convert it.
        conv_packet = weewx.units.to_std_system(event.packet, self.unit_system)
        # update the day cache
        self.day_cache.update(conv_packet, conv_packet['dateTime'])
        # extract and queue a max-min packet
        self.loop_queue.put(self.day_cache.get_max_min_packet(conv_packet['dateTime']))


# ============================================================================
#                         class MqttDashboardThread
# ============================================================================

class MqttDashboardThread(weewx.restx.RESTThread):
    """Class to publish data to a MQTT broker.

    Runs in a thread.
    """

    def __init__(self, queue, server_url,
                 client_id='', topic='', skip_upload=False,
                 augment_record=True, retain=False,
                 inputs={}, obs_to_upload='all',
                 manager_dict=None, tls=None, qos=0,
                 post_interval=None, stale=None,
                 log_success=True, log_failure=True,
                 timeout=60, max_tries=3, retry_wait=5,
                 max_backlog=sys.maxsize):

        # initialise our base class
        super(MqttDashboardThread, self).__init__(queue,
                                                  protocol_name='MQTT',
                                                  manager_dict=manager_dict,
                                                  post_interval=post_interval,
                                                  max_backlog=max_backlog,
                                                  stale=stale,
                                                  log_success=log_success,
                                                  log_failure=log_failure,
                                                  max_tries=max_tries,
                                                  timeout=timeout,
                                                  retry_wait=retry_wait)

        self.server_url = server_url
        self.client_id = client_id
        self.topic = topic
        self.tls_dict = {}
        if tls is not None:
            # we have TLS options so construct a dict to configure Paho TLS
            dflts = TLSDefaults()
            for opt in tls:
                if opt == 'cert_reqs':
                    if tls[opt] in dflts.CERT_REQ_OPTIONS:
                        self.tls_dict[opt] = dflts.CERT_REQ_OPTIONS.get(tls[opt])
                elif opt == 'tls_version':
                    if tls[opt] in dflts.TLS_VER_OPTIONS:
                        self.tls_dict[opt] = dflts.TLS_VER_OPTIONS.get(tls[opt])
                elif opt in dflts.TLS_OPTIONS:
                    self.tls_dict[opt] = tls[opt]
            logdbg("TLS parameters: %s" % self.tls_dict)
        self.retain = retain
        self.qos = qos
        self.skip_upload = skip_upload
        self.upload_all = True if obs_to_upload.lower() == 'all' else False
        self.inputs = inputs
        loginf("inputs=%s" % (self.inputs,))
        self.augment_record = augment_record
        self.templates = dict()

    def filter_data(self, record):
        """Filter records.

        Filter and format record contents before publishing. Returns a dict.
        """

        # if we are uploading all then return the record unchan ged
        if self.upload_all:
            return record
        else:
            # we are limiting the fields to those under [[[inputs]]]
            # first create a record with only usUnits, we don't want the user
            # to inadvertently leave usUnits out
            data = {'usUnits': record['usUnits']}
            # iterate over each of the keys in our input config, if that key is
            # in the record then add the record field to our dict
            for f in self.inputs:
                if f in record:
                    data[f] = record[f]
            # return our data
            return data

    def process_record(self, record, dbm):

        data = self.filter_data(record)
        if weewx.debug >= 2:
            logdbg("data: %s" % data)
        if self.skip_upload:
            loginf("skipping publication")
            return
        self.publish(json.dumps(data))

    def publish(self, data):
        """Publish an MQTT message.

        Attempt to publish 'data' to an MQTT broker. TLS is supported if
        configured as is retention and QoS. self.max_tries attempts will be
        made to publish before giving up.
        """

        url = urlparse(self.server_url)
        for _count in range(self.max_tries):
            try:
                client_id = self.client_id
                if not client_id:
                    pad = "%032x" % random.getrandbits(128)
                    client_id = 'weewx_%s' % pad[:8]
                mc = mqtt.Client(client_id=client_id)
                if url.username is not None and url.password is not None:
                    mc.username_pw_set(url.username, url.password)
                # if we have TLS opts configure TLS on our broker connection
                if len(self.tls_dict) > 0:
                    mc.tls_set(**self.tls_dict)
                mc.connect(url.hostname, url.port)
                mc.loop_start()
                (res, mid) = mc.publish(self.topic,
                                        data,
                                        retain=self.retain,
                                        qos=self.qos)
                if res != mqtt.MQTT_ERR_SUCCESS:
                    logerr("publish failed for %s: %s" % (self.topic, res))
                mc.loop_stop()
                mc.disconnect()
                return
            except (socket.error, socket.timeout, socket.herror) as e:
                logdbg("Failed publish attempt %d: %s" % (_count + 1, e))
            time.sleep(self.retry_wait)
        else:
            raise weewx.restx.FailedPost("Failed to publish after %d tries" %
                                         (self.max_tries,))


# ============================================================================
#                         class MQTTDashboardService
# ============================================================================

class MQTTDashboardService(weewx.engine.StdService):
    """Base class Service that publishes data to an MQTT broker.

    The MQTTDashboardSlow class creates and controls a threaded object of class
    MQTTDashboardSlowThread that generates slow changing JSON data once each archive
    period and publishes this data to a MQTT broker.

    MQTTDashboardSlow constructor parameters:

        engine:      a WeeWX engine, usually an instance of
                     weewx.engine.StdEngine
        config_dict: a WeeWX config dictionary

    MQTTDashboardSlow methods:

        new_archive_record. Action to be taken upon receipt of a new archive
                            record.
        ShutDown.           Shutdown any child threads.
    """

    def __init__(self, engine, config_dict, service_dict):
        # initialize my superclass
        super(MQTTDashboardService, self).__init__(engine, config_dict)

        # create a Queue object to pass data to our thread
        self.queue = Queue.Queue()

        # The service should bind itself to the NEW_LOOP_PACKET or
        # NEW_ARCHIVE_RECORD events respectively depending on whether the
        # service is processing loop packets or archive records. Example bindings are:
        # self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        # and/or
        # self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        # Generic methods that simply pass loop packets or archive records to
        # the service thread via a queue have been included. If more
        # sophisticated processing is required these methods should be
        # overridden as required.

    def new_archive_record(self, event):
        """Puts archive records in the queue."""

        self.queue.put(event.record)
        if weewx.debug >= 2:
            logdbg("Queued archive record dateTime=%s"
                   % timestamp_to_string(event.record['dateTime']))

    def new_loop_packet(self, event):
        """Puts loop packets in the queue."""

        self.queue.put(event.packet)
        if weewx.debug >= 2:
            logdbg("Queued loop packet dateTime=%s"
                   % timestamp_to_string(event.packet['dateTime']))

    def shutDown(self):
        """Shut down any threads."""

        if hasattr(self, 'queue') and hasattr(self, 'thread'):
            if self.queue and self.thread.is_alive():
                # put a None in the queue to signal the thread to shutdown
                self.queue.put(None)
                # wait up to 20 seconds for the thread to exit:
                self.thread.join(20.0)
                if self.thread.is_alive():
                    logerr("Unable to shut down %s thread" % self.thread.name)
                else:
                    logdbg("Shut down %s thread" % self.thread.name)


# ============================================================================
#                          class MqttDashboardAeris
# ============================================================================

class MqttDashboardAeris(MQTTDashboardService):
    """Service that publishes Aeris conditions and forecast data to a MQTT broker.

    The MqttDashboardAeris class creates and controls a threaded object of
    class MqttDashboardAerisThread that obtains current conditions and forecast
    data for a location from the Aeris API and publishes selected elements in
    JSON format to a MQTT broker.

    MqttDashboardAeris constructor parameters:

        engine:      a WeeWX engine, usually an instance of
                     weewx.engine.StdEngine
        config_dict: a WeeWX config dictionary

    MqttDashboardAeris methods:

        new_archive_record. Action to be taken upon receipt of a new archive
                            record. Inherited.
        ShutDown.           Shutdown any child threads. Inherited.
    """

    version = '0.1.0'

    def __init__(self, engine, config_dict):

        # first up say what we are loading
        loginf("Loading service MqttDashboardAeris v%s" % MqttDashboardAeris.version)

        # obtain config dicts for our service
        # first do we have a MQTTDashboard stanza in the config dict
        if 'MQTTDashboard' in config_dict:
            # we have a MQTTDashboard stanza now get the [[Aeris]] stanza as a
            # config dict
            aeris_config_dict = configobj.ConfigObj(config_dict['MQTTDashboard'].get('Aeris',
                                                                                     dict()))
            # get the [[[MQTT]]] stanza from the Aeris config dict
            mqtt_config_dict = configobj.ConfigObj(aeris_config_dict.get('MQTT', dict()))
            # merge in any MQTT config from [MQTTDashboard] [[MQTT]]
            conditional_merge(mqtt_config_dict, config_dict['MQTTDashboard'].get('MQTT',
                                                                                 dict()))
            # ensure we have any essential MQTT config, just call the essential
            # config elements and catch any KeyErrors
            try:
                _ = aeris_config_dict['client_id']
                _ = aeris_config_dict['client_secret']
            except KeyError as e:
                logerr("Service MqttDashboardAeris not loaded: Missing option %s" % (e,))
                return
        else:
            # if we have no MQTTDashboard config dict, ie [MQTTDashboard], we
            # can't go on so log the failure and return
            loginf("Service MqttDashboardAeris not loaded: missing [MQTTDashboard] stanza")
            return

        # initialize my superclass
        super(MqttDashboardAeris, self).__init__(engine, config_dict, aeris_config_dict)

        # get an instance of class MqttDashboardAerisThread and start the
        # thread running
        alt_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter').value
        self.thread = MqttDashboardAerisThread(self.queue,
                                               config_dict,
                                               aeris_config_dict,
                                               mqtt_config_dict,
                                               lat=engine.stn_info.latitude_f,
                                               long=engine.stn_info.longitude_f,
                                               alt_m=alt_m)
        self.thread.start()

        # bind ourself to the WeeWX NEW_ARCHIVE_RECORD event
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)


# ============================================================================
#                       class MqttDashboardAerisThread
# ============================================================================

class MqttDashboardAerisThread(threading.Thread):
    """Thread to obtain and Aeris API data and publish to a MQTT broker.

    The MqttDashboardAerisThread class queries the Aeris API and publishes
    selected forecast and current conditions data in JSON format to a MQTT
    broker. The Aeris API is called at a user selectable frequency. The thread
    listens for a shutdown signal from its parent.

    MqttDashboardAerisThread constructor parameters:

        queue:            A Queue object used to receive data from the parent.
        config_dict:      A WeeWX config dictionary.
        mw_config_dict:   A config dictionary for the MqttDashboardAerisThread.
        mqtt_config_dict: A config dictionary for the MQTT broker.
        lat:              Station latitude in decimal degrees.
        long:             Station longitude in decimal degrees.
        alt_m:            Station altitude in metres.

    MqttDashboardAerisThread methods:

        run.                  Monitor and act on data received via the queue.
        process_aeris.        Obtain forecast and current conditions data via
                              the Aeris API then process and publish selected
                              data in JSON format.
        is_night.             Determine if an archive record refers to
                              observations between sun set and sun rise.
        parse_aeris_response. Parse an Aeris API response and return selected
                              data.
    """

    # Define a dictionary to look up Aeris icon names and return corresponding
    # Saratoga icon code
    icon_dict = {
        'clear': 0,
        'cloudy': 18,
        'flurries': 25,
        'fog': 11,
        'hazy': 7,
        'mostlycloudy': 18,
        'mostlysunny': 9,
        'partlycloudy': 19,
        'partlysunny': 9,
        'sleet': 23,
        'rain': 20,
        'snow': 25,
        'sunny': 28,
        'tstorms': 29,
        'nt_clear': 1,
        'nt_cloudy': 13,
        'nt_flurries': 16,
        'nt_fog': 11,
        'nt_hazy': 13,
        'nt_mostlycloudy': 13,
        'nt_mostlysunny': 1,
        'nt_partlycloudy': 4,
        'nt_partlysunny': 1,
        'nt_sleet': 12,
        'nt_rain': 14,
        'nt_snow': 16,
        'nt_tstorms': 17,
        'chancerain': 20,
        'chancesleet': 23,
        'chancesnow': 25,
        'chancetstorms': 29
        }
    # icon dict to map Aeris icon names to existing icons
    aeris_icon_dict = {
        'blizzard.png': 'wi-day-snow-wind.svg',
        'blizzardn.png': 'wi-night-snow-wind.svg',
        'blowingsnow.png': 'wi-day-snow-wind.svg',
        'blowingsnown.png': 'wi-night-snow-wind.svg',
        'clear.png': 'wi-day-sunny.svg',
        'clearn.png': 'wi-night-clear.svg',
        'cloudy.png': 'wi-day-cloudy.svg',
        'cloudyn.png': 'wi-night-cloudy.svg',
        'cloudyw.png': 'wi-day-cloudy-gusts.svg',
        'cloudywn.png': 'wi-night-cloudy-gusts.svg',
        'cold.png': 'wi-snowflake-cold.svg',
        'coldn.png': 'wi-snowflake-cold.svg',
        'drizzle.png': 'wi-day-sprinkle.svg',
        'drizzlen.png': 'wi-night-sprinkle.svg',
        'dust.png': 'wi-dust.svg',
        'dustn.png': 'wi-dust.svg',
        'fair.png': 'wi-day-sunny-overcast.svg',
        'fairn.png': 'wi-night-cloudy.svg',
        'drizzlef.png': 'wi-sleet.svg',
        'fdrizzlen.png': 'wi-sleet.svg',
        'flurries.png': 'wi-day-snow.svg',
        'flurriesn.png': 'wi-night-snow.svg',
        'flurriesw.png': 'wi-day-snow-wind.svg',
        'flurrieswn.png': 'wi-night-snow-wind.svg',
        'fog.png': 'wi-day-fog.svg',
        'fogn.png': 'wi-night-fog.svg',
        'freezingrain.png': 'wi-day-sleet.svg',
        'freezingrainn.png': 'wi-night-sleet.svg',
        'hazy.png': 'wi-day-haze.svg',
        'hazyn.png': 'wi-night-cloudy-windy.svg',
        'hot.png': 'wi-hot.svg',
        'mcloudy.png': 'wi-cloudy.svg',
        'mcloudyn.png': 'wi-cloudy.svg',
        'mcloudyr.png': 'wi-day-rain.svg',
        'mcloudyrn.png': 'wi-night-rain.svg',
        'mcloudyrw.png': 'wi-day-rain-wind.svg',
        'mcloudyrwn.png': 'wi-night-rain-wind.svg',
        'mcloudys.png': 'wi-day-snow.svg',
        'mcloudysn.png': 'wi-night-snow.svg',
        'mcloudysf.png': 'wi-day-snow.svg',
        'mcloudysfn.png': 'wi-night-snow.svg',
        'mcloudysfw.png': 'wi-day-snow-wind.svg',
        'mcloudysfwn.png': 'wi-night-snow-wind.svg',
        'mcloudysw.png': 'wi-day-snow-wind.svg',
        'mcloudyswn.png': 'wi-night-snow-wind.svg',
        'mcloudyt.png': 'wi-day-thunderstorm.svg',
        'mcloudytn.png': 'wi-night-thunderstorm.svg',
        'mcloudytw.png': 'wi-day-thunderstorm.svg',
        'mcloudytwn.png': 'wi-night-thunderstorm.svg',
        'mcloudyw.png': 'wi-cloudy-gusts.svg',
        'mcloudywn.png': 'wi-night-cloudy-gusts.svg',
        'na.png': 'wi-na.svg',
        'pcloudy.png': 'wi-day-sunny-overcast.svg',
        'pcloudyn.png': 'wi-night-partly-cloudy.svg',
        'pcloudyr.png': 'wi-day-rain.svg',
        'pcloudyrn.png': 'wi-night-rain.svg',
        'pcloudyrw.png': 'wi-day-rain-wind.svg',
        'pcloudyrwn.png': 'wi-night-rain-wind.svg',
        'pcloudys.png': 'wi-day-snow.svg',
        'pcloudysn.png': 'wi-night-snow.svg',
        'pcloudysf.png': 'wi-day-snow.svg',
        'pcloudysfn.png': 'wi-night-snow.svg',
        'pcloudysfw.png': 'wi-day-snow-wind.svg',
        'pcloudysfwn.png': 'wi-night-snow-wind.svg',
        'pcloudysw.png': 'wi-day-snow-wind.svg',
        'pcloudyswn.png': 'wi-night-snow-wind.svg',
        'pcloudyt.png': 'wi-day-thunderstorm.svg',
        'pcloudytn.png': 'wi-night-thunderstorm.svg',
        'pcloudytw.png': 'wi-day-thunderstorm.svg',
        'pcloudytwn.png': 'wi-night-thunderstorm.svg',
        'pcloudyw.png': '.svg',
        'pcloudywn.png': '.svg',
        'rain.png': 'wi-day-rain.svg',
        'rainn.png': 'wi-night-rain.svg',
        'rainandsnow.png': 'wi-day-sleet.svg',
        'rainandsnown.png': 'wi-night-sleet.svg',
        'raintosnow.png': 'wi-day-sleet.svg',
        'raintosnown.png': 'wi-night-sleet.svg',
        'rainw.png': 'wi-day-rain-wind.svg',
        'rainwn.png': 'wi-night-rain-wind.svg',
        'showers.png': 'wi-day-showers.svg',
        'showersn.png': 'wi-night-showers.svg',
        'showersw.png': 'wi-day-rain-mix.svg',
        'showerswn.png': 'wi-night-rain-mix.svg',
        'sleet.png': 'wi-day-sleet.svg',
        'sleetn.png': 'wi-night-sleet.svg',
        'sleetsnow.png': 'wi-day-sleet.svg',
        'sleetsnown.png': 'wi-night-sleet.svg',
        'smoke.png': 'wi-smoke.svg',
        'smoken.png': 'wi-smoke.svg',
        'snow.png': 'wi-day-snow.svg',
        'snown.png': 'wi-night-snow.svg',
        'snoww.png': 'wi-day-snow-wind.svg',
        'snowwn.png': 'wi-night-snow-wind.svg',
        'snowshowers.png': 'wi-day-rain-mix.svg',
        'snowshowersn.png': 'wi-night-rain-mix.svg',
        'snowshowersw.png': 'wi-day-rain-mix.svg',
        'snowshowerswn.png': 'wi-night-rain-mix.svg',
        'snowtorain.png': 'wi-day-rain-mix.svg',
        'snowtorainn.png': 'wi-night-rain-mix.svg',
        'sunny.png': 'wi-day-sunny.svg',
        'sunnyn.png': 'wi-night-clear.svg',
        'sunnyw.png': 'wi-day-windy.svg',
        'sunnywn.png': 'wi-night-clear.svg',
        'tstorm.png': 'wi-day-thunderstorm.svg',
        'tstormn.png': 'wi-night-thunderstorm.svg',
        'tstorms.png': 'wi-day-snow-thunderstorm.svg',
        'tstormsn.png': 'wi-night-snow-thunderstorm.svg',
        'tstormsw.png': 'wi-day-thunderstorm.svg',
        'tstormswn.png': 'wi-night-thunderstorm.svg',
        'wind.png': 'wi-day-windy.svg',
        'windn.png': 'wi-night-clear.svg',
        'wintrymix.png': 'wi-day-rain-mix.svg',
        'wintrymixn.png': 'wi-night-rain-mix.svg',
    }
    endpoint_config = {
        'forecasts': {
            'parameters': {
                # 'fields': 'periods.timestamp,periods.validTime,periods.weather,periods.weatherPrimary,periods.icon,periods.maxTemp',
                'filter': 'daynight',
                'format': 'json',
                'limit': 5
            }
        },
        'observations': {
            'parameters': {
                'fields': 'periods.timestamp',
                'format': 'json'
            }
        }
    }

    def __init__(self, queue, config_dict, aeris_config_dict, mqtt_config_dict,
                 lat, long, alt_m):

        # Initialize my superclass
        threading.Thread.__init__(self)

        self.setName('MqttDashboardAerisThread')
        self.setDaemon(True)
        self.queue = queue

        # Get station info required for Sun related calcs
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt_m

        # MQTT broker URL
        server_url = mqtt_config_dict.get('server_url')
        # topics to publish to
        self.topic = dict()
        self.topic['observations'] = mqtt_config_dict.get('conditions_topic', 'weather/conditions')
        self.topic['forecasts'] = mqtt_config_dict.get('forecast_topic', 'weather/forecast')
        # TLS options
        tls_opt = mqtt_config_dict.get('tls', None)
        # quality of service
        qos = to_int(mqtt_config_dict.get('tls', 0))
        # will MQTT broker retain messages
        retain = to_bool(mqtt_config_dict.get('retain', True))
        # log successful publishing of data
        log_success = to_bool(mqtt_config_dict.get('log_success', False))

        # do our config logging before the Publisher
        loginf("Data will be published to %s" % obfuscate_password(server_url))
        loginf("Conditions will be published to topic '%s'" % self.topic['observations'])
        loginf("Forecast will be published to topic '%s'" % self.topic['forecasts'])
        loginf("qos is %d, retain is %r, log success is %s" % (qos, retain, log_success))

        # get a MQTTPublisher object to do the publishing for us
        self.publisher = MqttPublisher(server_url=server_url,
                                       tls=tls_opt,
                                       retain=retain,
                                       qos=qos,
                                       log_success=log_success)

        # list of the Aeris API 'endpoints' to be used
#        self.endpoints = ['observations', 'forecasts']
        self.endpoints = ['forecasts']
        # interval between API calls for each 'endpoint'
        self.interval = dict()
        self.interval['observations'] = to_int(aeris_config_dict.get('current_interval', 1800))
        self.interval['forecasts'] = to_int(aeris_config_dict.get('forecast_interval', 1800))
        # max no of tries we will make in any one attempt to contact WU via API
        self.max_aeris_tries = to_int(aeris_config_dict.get('max_WU_tries', 3))
        # Get API call lockout period. This is the minimum period between API
        # calls for the same feature. This prevents an error condition making
        # multiple rapid API calls and thus break the API usage conditions.
        self.lockout_period = to_int(aeris_config_dict.get('api_lockout_period', 60))
        # initialise containers for timestamp of last API call made for each
        # feature
        self.last = dict()
        self.last['observations'] = None
        self.last['forecasts'] = None
        # initialise container for timestamp of last Aeris API call
        self.last_call_ts = None
        # Get our API key from weewx.conf, first look in [Weewx-WD] and if no luck
        # try [Forecast] if it exists. Wrap in a try..except loop to catch exceptions (ie one or
        # both don't exist.
        client_id = aeris_config_dict.get('client_id')
        if client_id is None:
            raise MissingApiKey("Cannot find valid Aeris client ID")
        client_secret = aeris_config_dict.get('client_secret')
        if client_secret is None:
            raise MissingApiKey("Cannot find valid Aeris client secret")
        # Get 'action' (ie the location) to be used for use in API calls.
        # Refer weewx.conf for details.
        self.query = aeris_config_dict.get('location', (lat, long))
        # get an AerisAPI object to handle the API calls
        self.api = AerisAPI(client_id, client_secret)

        # initialise night flag
        self.night = None

    def run(self):
        """Collect records from the queue and manage their processing."""

        try:
            # Run a continuous loop, processing data received in the queue. Only
            # break out if we receive the shutdown signal (None) from our parent.
            while True:
                # Run an inner loop checking for the shutdown signal and keeping
                # the queue length from getting too long. If an archive record is
                # received break out of the loop and process it
                while True:
                    _package = self.queue.get()
                    if _package is None:
                        # None is our signal to exit
                        # disconnect cleanly if we have already connected
                        if self.publisher:
                            self.publisher.disconnect()
                        return
                    # if packets have backed up in the queue, trim it until it's no
                    # bigger than the max allowed backlog
                    if self.queue.qsize() <= 5:
                        break

                # we now have a record to process
                # First, log receipt. The amount of logging depends on the debug
                # level.
                if weewx.debug >= 2:
                    logdbg("Received archive record")
                elif weewx.debug >= 3:
                    logdbg("Received archive record: %s" % (_package, ))
                # process the record
                self.process_aeris(_package)
        except Exception as e:
            # Some unknown exception occurred. This is probably a serious
            # problem. Exit.
            logcrit("Unexpected exception of type %s" % (type(e), ))
            log_traceback_critical('mqttdashboardaeristhread: **** ')
            logcrit("Thread exiting. Reason: %s" % (e, ))

    def process_aeris(self, record):
        """Process the Aeris API data then publish to a MQTT broker."""

        # determine if it is night or day so we can set day or night icons
        # when translating Aeris icons to Saratoga icons
        self.night = self.is_night(record)
        # loop through our list of API calls to be made
        for endpoint in self.endpoints:
            # get the current archive record time
            now = int(record['dateTime'])
            if weewx.debug >= 2:
                logdbg("Last Aeris %s API call at %s" % (endpoint,
                                                         self.last[endpoint]))
            # has the lockout period passed since the last call of this
            # feature?
            if self.last_call_ts is None or ((now + 1 - self.lockout_period) >= self.last_call_ts):
                # If we haven't made this API call previously or if its been
                # too long since the last call then make the call
                if (self.last[endpoint] is None) or \
                        ((now + 1 - self.interval[endpoint]) >= self.last[endpoint]):
                    # Make the call, wrap in a try..except just in case
                    try:
                        response = self.api.request(endpoint=endpoint,
                                                    action=self.query,
                                                    parameters=self.endpoint_config[endpoint].get('parameters', {}),
                                                    max_tries=self.max_aeris_tries)
                        logdbg("Downloaded updated Aeris %s information" % endpoint)
                    except Exception as e:
                        # Some unknown exception occurred. Log it and continue.
                        loginf("Unexpected exception of type %s" % (type(e), ))
                        weeutil.weeutil.log_traceback('mqttdashboardaeristhread: **** ')
                        loginf("Unexpected exception of type %s" % (type(e), ))
                        loginf("Aeris '%s' API query failed" % endpoint)
                        continue
                    # if we got something back then reset our timestamp for
                    # this feature
                    if response is not None:
                        self.last[endpoint] = now
                    # parse the Aeris response and create a dict to publish
                    _data = self.parse_aeris_response(endpoint, response, now)

                    # timestamp the data to be posted
                    _data['last_updated'] = now
                    # connect to our mqtt broker
                    self.publisher.connect()
                    # publish to MQTT broker
                    self.publisher.publish(self.topic[endpoint],
                                           json.dumps(_data),
                                           now)
            else:
                # API call limiter kicked in so say so
                loginf("Tried to make an API call within %d sec "
                       "of the previous call." % (self.lockout_period, ))
                loginf("API call limit reached. API call skipped.")
                continue
        # get the last API call timestamp
        self.last_call_ts = max(self.last[q] for q in self.last if self.last[q] is not None)

    def is_night(self, record):
        """Given a WeeWX archive record determine if it is night.

        Calculates sun rise and sun set and determines whether the dateTime
        field in the record concerned falls outside of the period sun rise to
        sun set.

        Input:
            record: a WeeWX archive record or loop packet.

        Returns:
            False if the dateTime field is during the daytime otherwise True.
        """

        # Almanac object gives more accurate results if current temp and
        # pressure are provided. Initialise some defaults.
        temperature_c = 15.0
        pressure_mbar = 1010.0
        # get current outTemp and barometer if they exist
        if 'outTemp' in record:
            temperature_c = weewx.units.convert(weewx.units.as_value_tuple(record, 'outTemp'),
                                                "degree_C").value
        if 'barometer' in record:
            pressure_mbar = weewx.units.convert(weewx.units.as_value_tuple(record, 'barometer'),
                                                "mbar").value
        # get our almanac object
        almanac = weewx.almanac.Almanac(record['dateTime'],
                                        self.latitude,
                                        self.longitude,
                                        self.altitude_m,
                                        temperature_c,
                                        pressure_mbar)
        # work out sunrise and sunset timestamp so we can determine if it is
        # night or day
        sunrise_ts = almanac.sun.rise.raw
        sunset_ts = almanac.sun.set.raw
        # if we are not between sunrise and sunset it must be night
        return not (sunrise_ts < record['dateTime'] < sunset_ts)

    def parse_aeris_response(self, endpoint, json_response, now_ts):
        """ Parse an Aeris response and return required fields.

        Take a Aeris API response for a given 'feature', check for (Aeris
        defined) errors then extract and return the fields of interest.

        Inputs:
            feature: The Aeris data set or 'feature' to be parsed. String.
                     (refer https://www.wunderground.com/weather/api/d/docs?d=index)
            response: A Aeris API response in JSON format.

        Returns:
            A dictionary containing the fields of interest from the Aeris API
            response.
        """

        # create a holder for the data we gather
        data = {}
        # check for recognised format
        if 'response' not in json_response:
            loginf("Unknown format in Aeris '%s'" % (endpoint,))
            return data
        # no Aeris error so start pulling in the data we want
        if endpoint == 'forecasts':
            # we have forecast data
            period = 0
            fcast_periods_data = json_response['response'][0]['periods']
            for fcast_period in fcast_periods_data:
                data[period] = {}
                ts_dt = datetime.datetime.fromtimestamp(fcast_period['timestamp'])
                if fcast_period['isDay']:
                    data[period]['title'] = ts_dt.strftime('%A')
                else:
                    data[period]['title'] = '%s night' % ts_dt.strftime('%A')
                data[period]['forecastIcon'] = self.aeris_icon_dict.get(fcast_period['icon'],
                                                                        'na.png')
                data[period]['forecastText'] = fcast_period['weather']
                data[period]['forecastTextMetric'] = '. '.join([fcast_period['weather'],
                                                                self.generate_forecast_text(fcast_period)])
                period += 1
        elif endpoint == 'conditions':
            # we have conditions data
            _current = json_response['current_observation']
            # Aeris? does not seem to provide day/night icon name in their
            # 'conditions' response so we need to do. Just need to add 'nt_'
            # to front of name before looking up in out Saratoga icons
            # dictionary
            if self.night:
                data['currentIcon'] = 'nt_' + _current['icon']
            else:
                data['currentIcon'] = _current['icon']
            data['currentText'] = _current['weather']
        return data

    @staticmethod
    def generate_forecast_text(data, units='metric'):
        """Generate supplementary forecast text from forecast obs."""

        # generate high/low string
        str_list = []
        if 'maxTempC' in data and data['maxTempC'] is not None:
            str_list.append('High %d°C' % data['maxTempC'])
        if 'minTempC' in data and data['minTempC'] is not None:
            str_list.append('Low %d°C' % data['minTempC'])
        temp_str = ', '.join(str_list)
        # generate rain string
        str_list = []
        if 'pop' in data and data['pop'] is not None:
            str_list.append('Chance of precipitation %d%%' % data['pop'])
            if 'precipMM' in data and data['precipMM'] is not None and data['pop'] > 0:
                str_list.append('(%dmm)' % data['precipMM'])
        rain_str = ' '.join(str_list)
        # UV index string
        if 'uvi' in data and data['uvi'] is not None and data['isDay']:
            uv_str = 'UV index %d.' % data['uvi']
        else:
            uv_str = ''
        # wind string
        if 'windDir' in data and data['windDir'] is not None \
                and 'windSpeedKPH' in data and data['windSpeedKPH'] is not None \
                and 'windGustKPH' in data and data['windGustKPH'] is not None:
            wind_str = 'Wind %s %d to %d km/h' % (data['windDir'],
                                                  data['windSpeedKPH'],
                                                  data['windGustKPH'])
        else:
            wind_str = ''
        return '. '.join([temp_str, rain_str, wind_str, uv_str])


# ============================================================================
#                               class AerisAPI
# ============================================================================

class AerisAPI(object):
    """Query the Aeris API and return the API response.

    Data is obtained from the Aeris API by accessing one or more endpoints. The
    available endpoints depend on the user's subscription level. This class
    supports access to a limited suite of Aeris API endpoints to support
    various WeeWX extensions.

    AerisAPI constructor parameters:

        api_key: WeatherUnderground API key to be used.

    AerisAPI methods:

        data_request. Submit a data feature request to the WeatherUnderground
                      API and return the response.
    """

    BASE_URL = 'https://api.aerisapi.com'
    SUPPORTED_ENDPOINTS = ['forecasts', 'observations']

    def __init__(self, client_id, client_secret):
        # initialise a AerisAPI object

        # save the client ID and secret to be used
        self.client_id = client_id
        self.client_secret = client_secret

    def request(self, endpoint=None, action=None, parameters=None, max_tries=3):
        """Request data from and endpoint.

        Construct the URL to be used for an API call, make the call and return
        the response.

        Parameters:
            endpoint:    The Aeris API endpoint to be used. String.
            action:      A modifier to support the endpoint, for example a
                         location for which the data is require. String.
            parameters:  Optional parameters to be to be included in the API
                         call eg filter to filter to limit the results.
                         Dictionary with elements in the format parameter: value.
            max_tries:   The maximum number of attempts to be made to obtain a
                         response from the API. Default is 3.

        Returns:
            The API response in JSON format or the value None if the requested
            data could not be obtained.
        """

        # do we have an endpoint and is it an endpoint we know about
        if endpoint is not None and endpoint.lower() in AerisAPI.SUPPORTED_ENDPOINTS:
            action_str = quote('/%s' % action) if action is not None else ''
            client_dict = {'client_id': self.client_id,
                           'client_secret': self.client_secret}
            if parameters is not None:
                try:
                    client_dict.update(parameters)
                    params_dict = client_dict
                except (TypeError, AttributeError):
                    logdbg("Ignoring invalid API query parameter format: '%s'" % (parameters,))
                    params_dict = client_dict
            else:
                params_dict = client_dict
            params = urlencode(params_dict)
            url = "%s/%s%s?%s" % (AerisAPI.BASE_URL,
                                  endpoint,
                                  action_str,
                                  params)
            # obtain a version of the url with client_id and client_secret
            # obfuscated
            obf_url = obfuscate_client(url)
            # log the url used
            logdbg("Submitting Aeris API call using URL: %s" % (obf_url,))
            # we will attempt the call max_tries times
            for count in range(max_tries):
                # attempt the call
                try:
                    request = urlopen(url)
                    response = request.read()
                except (URLError, socket.timeout) as e:
                    logerr("Failed to get endpoint '%s' on attempt %d" % (endpoint,
                                                                          count + 1))
                    logerr("   **** %s" % e)
                else:
                    response_json = json.loads(response)
                    if response_json['success']:
                        return response_json
                    else:
                        loginf("An error was encountered in the API response: %s" % (response_json['error']['description']))
                        request.close()
                        continue
            else:
                logerr("Failed to get endpoint '%s'" % endpoint)
            return None
        else:
            # either no endpoint was provided or we don't know how to process
            # that endpoint, either way log it and return None
            logdbg("Invalid or unsupported endpoint '%s' provided" % (endpoint,))

# ============================================================================
#                             class MQTTDashboardSlow
# ============================================================================

class MqttDashboardSlow(MQTTDashboardService):
    """Service that publishes slow changing JSON format data to an MQTT broker.

    The MQTTDashboardSlow class creates and controls a threaded object of class
    MQTTDashboardSlowThread that generates slow changing JSON data once each archive
    period and publishes this data to a MQTT broker.

    MQTTDashboardSlow constructor parameters:

        engine:      a WeeWX engine, usually an instance of
                     weewx.engine.StdEngine
        config_dict: a WeeWX config dictionary

    MQTTDashboardSlow methods:

        new_archive_record. Action to be taken upon receipt of a new archive
                            record.
        ShutDown.           Shutdown any child threads.
    """
    version = '0.1.0'

    def __init__(self, engine, config_dict):

        # first up say what we are loading
        loginf("Loading service MqttDashboardSlow v%s" % MqttDashboardSlow.version)

        # obtain config dicts for our service
        # first do we have a MQTTDashboard stanza in the config dict
        if 'MQTTDashboard' in config_dict:
            # we have a MQTTDashboard stanza now get the [[Slow]] stanza as a
            # config dict
            slow_config_dict = configobj.ConfigObj(config_dict['MQTTDashboard'].get('Slow',
                                                                                    dict()))
            # get the [[[MQTT]]] stanza from the Slow config dict
            mqtt_config_dict = configobj.ConfigObj(slow_config_dict.get('MQTT', dict()))
            # merge in any MQTT config from [MQTTDashboard] [[MQTT]]
            conditional_merge(mqtt_config_dict, config_dict['MQTTDashboard'].get('MQTT',
                                                                                 dict()))
            # ensure we have any essential MQTT config, just call the essential
            # config elements and catch any KeyErrors
            try:
                _ = mqtt_config_dict['server_url']
                _ = mqtt_config_dict['topic']
            except KeyError as e:
                logerr("Service MqttDashboardSlow not loaded: Missing option %s" % (e,))
                return
        else:
            # if we have no MQTTDashboard config dict, ie [MQTTDashboard], we
            # can't go on so log the failure and return
            loginf("Service MqttDashboardSlow not loaded: missing [MQTTDashboard] stanza")
            return

        # initialize my superclass
        super(MqttDashboardSlow, self).__init__(engine, config_dict, slow_config_dict)

        # get an instance of class MQTTDashboardSlowThread and start the thread
        # running
        alt_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter').value
        self.thread = MqttDashboardSlowThread(self.queue,
                                              config_dict,
                                              slow_config_dict,
                                              mqtt_config_dict,
                                              lat=engine.stn_info.latitude_f,
                                              long=engine.stn_info.longitude_f,
                                              alt_m=alt_m)
        self.thread.start()

        # bind ourself to the WeeWX NEW_ARCHIVE_RECORD event
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)


# ============================================================================
#                        class MQTTDashboardSlowThread
# ============================================================================

class MqttDashboardSlowThread(threading.Thread):
    """Thread that publishes slow changing JSON format data to a MQTT broker.

    The MQTTDashboardSlowThread class accepts archive records via a Queue object,
    creates a dictionary of selected obs/aggregates and publishes this data in
    JSON format to a MQTT broker. The thread listens for a shutdown signal from
    its parent.

    MQTTDashboardSlowThread constructor parameters:

        queue:            A Queue object used to receive data from the parent.
        config_dict:      A WeeWX config dictionary.
        ma_config_dict:   A config dictionary for the MQTTDashboardSlowThread.
        mqtt_config_dict: A config dictionary for the MQTT broker.
        lat:              Station latitude in decimal degrees.
        long:             Station longitude in decimal degrees.
        alt_m:            Station altitude in metres.

    MQTTDashboardSlowThread methods:

        run.            Finalise some initialisation actions then monitor and
                        act on data received via the queue.
        process_record. Process a received archive record, obtain the data to
                        be published and publish the data.
        calculate.      Calculate/generate the data to be published.
    """

    def __init__(self, queue, config_dict, slow_config_dict, mqtt_config_dict,
                 lat, long, alt_m):

        # initialize my superclass
        threading.Thread.__init__(self)

        self.setName('MqttDashboardSlowThread')
        self.setDaemon(True)
        self.queue = queue
        self.config_dict = config_dict

        # various dicts used later with converters and formatters
        self.group_dict = slow_config_dict.get('Groups', weewx.units.MetricUnits)
        self.format_dict = slow_config_dict.get('Formats',
                                                weewx.units.default_unit_format_dict)
        self.moonphases = slow_config_dict.get('Almanac', {}).get('moon_phases',
                                                                  weeutil.Moon.moon_phases)

        # get MQTT config options
        server_url = mqtt_config_dict.get('server_url', None)
        self.topic = mqtt_config_dict.get('topic', 'weather/slow')
        tls_opt = mqtt_config_dict.get('tls', None)
        qos = to_int(mqtt_config_dict.get('tls', 0))
        retain = to_bool(mqtt_config_dict.get('retain', True))
        log_success = to_bool(mqtt_config_dict.get('log_success', True))
        mqtt_debug = to_int(mqtt_config_dict.get('mqtt_debug', 0))

        # do our config logging before the Publisher
        loginf("Data will be published to %s" % obfuscate_password(server_url))
        loginf("Topic is '%s'" % self.topic)
        loginf("qos is %d, retain is %r, log success is %s" % (qos, retain, log_success))

        # get an MQTTPublisher object to do the publishing for us
        self.publisher = MqttPublisher(server_url=server_url,
                                       tls=tls_opt,
                                       retain=retain,
                                       qos=qos,
                                       log_success=log_success,
                                       mqtt_debug=mqtt_debug)

        # Set the binding to be used for data from an additional (ie not the
        # [StdArchive]) binding. Default to 'wx_binding'.
        self.additional_binding = slow_config_dict.get('additional_binding',
                                                       'wx_binding')

        # get some station info
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt_m

        # initialise some properties that will pickup real values later
        self.db_manager = None
        self.additional_manager = None
        self.stats = None
        self.db_binder = None
        self.converter = None
        self.formatter = None
        self.almanac = None

    def run(self):
        """Collect records from the queue and manage their processing.

        Now that we are in a thread get a manager for our dbs (bindings). Once
        this is done we wait for something in the queue.
        """

        # Would normally do this in our class' __init__ but since we are are
        # running in a thread we need to wait until the thread is actually
        # running before we can get any db related objects.

        # get a DBBinder object
        self.db_binder = weewx.manager.DBBinder(self.config_dict)

        # get Converter and Formatter objects to convert/format our data
        self.converter = weewx.units.Converter(self.group_dict)
        self.formatter = weewx.units.Formatter(unit_format_dict=self.format_dict)

        # Run a continuous loop, processing data received in the queue. Only
        # break out if we receive the shutdown signal (None) from our parent.
        while True:
            # Run an inner loop checking for the shutdown signal and keeping
            # the queue length from getting too long. If an archive record is
            # received break out of the loop and process it
            while True:
                _package = self.queue.get()
                if _package is None:
                    # None is our signal to exit
                    # disconnect cleanly from the broker if we have already
                    # connected
                    if self.publisher:
                        self.publisher.disconnect()
                    return
                # if packets have backed up in the queue, trim it until it's no
                # bigger than the max allowed backlog
                if self.queue.qsize() <= 5:
                    break

            # we now have a record to process
            # first, log receipt
            if weewx.debug >= 2:
                logdbg("Received archive record")
            elif weewx.debug > 2:
                logdbg("Received archive record: %s" % (_package, ))
            # process the record
            self.process_record(_package)

    def process_record(self, record):
        """Process incoming record, generate and post the JSON data.

        Parameters:
            record: dict containing the just received archive record
        """

        # get time for debug timing
        t1 = time.time()

        # since we are in a thread include some robust error catching and
        # reporting so we don't just silently die
        try:
            db_lookup = self.db_binder.bind_default('wx_binding')
            trend_dict = {'time_delta': 10800,
                          'time_grace': 300}
            self.stats = weewx.tags.TimeBinder(db_lookup,
                                               record['dateTime'],
                                               converter=self.converter,
                                               formatter=self.formatter,
                                               trend=trend_dict)
            temperature_c = record.get('outTemp', 15.0)
            pressure_mbar = record.get('barometer', 1010.0)
            self.almanac = weewx.almanac.Almanac(record['dateTime'],
                                                 self.latitude,
                                                 self.longitude,
                                                 altitude=self.altitude_m,
                                                 temperature=temperature_c,
                                                 pressure=pressure_mbar,
                                                 moon_phases=self.moonphases,
                                                 formatter=self.formatter)
            # get a data dict from which to construct our JSON data
            data = self.calculate(record)
            # connect to the mqtt broker
            self.publisher.connect()
            # publish the data
            self.publisher.publish(self.topic,
                                   json.dumps(data),
                                   data['dateTime']['now'])
            # log the time taken to process this record
            logdbg("Record (%s) processed in %.5f seconds" % (record['dateTime'],
                                                              (time.time()-t1)))
        except FailedPost as e:
            # data could not be published, log and continue
            logerr("Data was not published: %s" % (e, ))
        except Exception as e:
            # Some unknown exception occurred. This is probably a serious
            # problem. Exit.
            logcrit("Unexpected exception of type %s" % (type(e), ))
            log_traceback_critical('mqttarchivethread: **** ')
            logcrit("Thread exiting. Reason: %s" % (e, ))

    def calculate(self, record):
        """Construct a data dict to be used as the JSON source.

        Parameters:
            record: loop data record

        Returns:
            Dictionary containing the data to be posted.
        """

        packet_d = dict(record)
        ts = int(packet_d['dateTime'])

        # initialise our result containing dict
        data = {}

        # Add dateTime fields
        datetime = dict()
        # now
        datetime['now'] = ts
        # add dateTime fields to our data
        data['dateTime'] = datetime

        # Add outTemp fields
        outtemp = dict()
        # yesterday
        outtemp['yest'] = dict()
        outtemp['yest']['min'] = to_float(self.stats.yesterday().outTemp.min.formatted)
        outtemp['yest']['min_ts'] = self.stats.yesterday().outTemp.mintime.raw
        outtemp['yest']['max'] = to_float(self.stats.yesterday().outTemp.max.formatted)
        outtemp['yest']['max_ts'] = self.stats.yesterday().outTemp.maxtime.raw
        # trend
        outtemp['trend'] = to_float(self.stats.trend(time_delta=3600).outTemp.formatted)
        outtemp['24h_trend'] = to_float(self.stats.trend(time_delta=86400).outTemp.formatted)
        # add outTemp fields to our data
        data['outTemp'] = outtemp

        # Add barometer fields
        barometer = dict()
        # trend
        barometer['trend'] = to_float(self.stats.trend(time_delta=10800).barometer.formatted)
        # add barometer fields to our data
        data['barometer'] = barometer

        # Add dewpoint fields
        dewpoint = dict()
        # trend
        dewpoint['trend'] = to_float(self.stats.trend(time_delta=3600).dewpoint.formatted)
        # add dewpoint fields to our data
        data['dewpoint'] = dewpoint

        # Add outHumidity fields
        outhumidity = dict()
        # trend
        outhumidity['trend'] = to_float(self.stats.trend(time_delta=3600).outHumidity.formatted)
        # add outHumidity fields to our data
        data['outHumidity'] = outhumidity

        # Add wind fields
        wind = dict()
        # windGust
        wind['windGust'] = dict()
        # yesterday
        wind['windGust']['yest'] = dict()
        wind['windGust']['yest']['max'] = to_float(self.stats.yesterday().windGust.max.formatted)
        wind['windGust']['yest']['max_ts'] = self.stats.yesterday().windGust.maxtime.raw
        # windrun
        wind['windrun'] = dict()
        if self.stats.yesterday().windrun.exists:
            _run = to_float(self.stats.yesterday().windrun.sum.formatted)
        else:
            try:
                _run_km = self.stats.yesterday().windSpeed.avg.km_per_hour.raw * 24
                _run_vt = ValueTuple(_run_km, 'km', 'group_distance')
                _run_c = self.converter.convert(_run_vt)
                _run = to_float(self.formatter.toString(_run_c, addLabel=False))
            except TypeError:
                _run = None
        wind['windrun']['yest'] = _run
        # add wind fields to our data
        data['wind'] = wind

        # Add rain fields
        rain = dict()
        # yesterday
        rain['yest'] = to_float(self.stats.yesterday().rain.sum.formatted)
        # month to date
        rain['mtd'] = to_float(self.stats.month().rain.sum.formatted)
        # year to date
        rain['ytd'] = to_float(self.stats.year().rain.sum.formatted)
        # add rain fields to our data
        data['rain'] = rain

        # Add radiation fields
        radiation = dict()
        # yesterday
        radiation['yest'] = dict()
        radiation['yest']['max'] = to_float(self.stats.yesterday().radiation.max.formatted)
        radiation['yest']['max_ts'] = self.stats.yesterday().radiation.maxtime.raw
        # add radiation fields to our data
        data['radiation'] = radiation

        # Add UV fields
        uv = dict()
        # yesterday
        uv['yest'] = dict()
        uv['yest']['max'] = to_float(self.stats.yesterday().UV.max.formatted)
        uv['yest']['max_ts'] = self.stats.yesterday().UV.maxtime.raw
        # add UV fields to our data
        data['UV'] = uv

        # Add sun fields
        sun = dict()
        # rise, set and day length
        sun['rise'] = self.almanac.sun.rise.raw
        sun['set'] = self.almanac.sun.set.raw
        if self.almanac.hasExtras:
            sun['dayLength'] = (self.almanac(pressure=0, horizon=-34.0/60).sun.set.raw -
                                self.almanac(pressure=0, horizon=-34.0/60).sun.rise.raw)
        else:
            sun['dayLength'] = sun['set'] - sun['rise']
        # add sun fields to our data
        data['sun'] = sun

        # Add moon fields
        moon = dict()
        # rise and set
        if self.almanac.hasExtras:
            moon['rise'] = self.almanac.moon.rise.raw
            moon['set'] = self.almanac.moon.set.raw
            prev_phases = []
            next_phases = []
            prev_phases.append(["Full Moon",
                                self.almanac.previous_full_moon.raw])
            prev_phases.append(["Last Quarter",
                                self.almanac.previous_last_quarter_moon.raw])
            prev_phases.append(["New Moon",
                                self.almanac.previous_new_moon.raw])
            prev_phases.append(["First Quarter",
                                self.almanac.previous_first_quarter_moon.raw])
            prev_ph = max(prev_phases, key=operator.itemgetter(1))
            next_phases.append(["Full Moon",
                                self.almanac.next_full_moon.raw])
            next_phases.append(["Last Quarter",
                                self.almanac.next_last_quarter_moon.raw])
            next_phases.append(["New Moon",
                                self.almanac.next_new_moon.raw])
            next_phases.append(["First Quarter",
                                self.almanac.next_first_quarter_moon.raw])
            # get the next 4 phases in chrono order
            _sorted = sorted(next_phases, key=operator.itemgetter(1))
            moon['lastPhase'] = dict()
            moon['lastPhase']['name'] = prev_ph[0]
            moon['lastPhase']['date'] = prev_ph[1]
            moon['nextPhases'] = dict()
            moon['nextPhases'] = _sorted
        else:
            # no pyephem so no moon data
            moon['rise'] = None
            moon['set'] = None
            moon['lastPhase'] = dict()
            moon['lastPhase']['name'] = None
            moon['lastPhase']['date'] = None
            moon['nextPhases'] = dict()
        # add moon fields to our data
        data['moon'] = moon

        return data


# ============================================================================
#                              class MqttPublisher
# ============================================================================

class MqttPublisher(object):
    """Class to publish data to a MQTT broker.

    Class used to publish data to a MQTT broker. To use, instantiate an
    MQTTPublisher object and then call the publish() method with the data to be
    published as the only parameter. A weewx.restx.FailedPost exception is
    raised if the data could not be published.
    """

    def __init__(self, server_url, client_id=None, tls=None, retain=False,
                 qos=0, timeout=3, max_tries=3, retry_wait=3,
                 log_success=True, mqtt_debug=0):

        # URL of the MQTT broker to be used
        self.server_url = server_url
        # obtain the client ID to be used, if not provided (ie is None)
        # generate a 32 digit random hexadecimal number and append the first
        # eight characters of the hexadecimal number to the stem 'weewx_' to
        # derive the client ID
        _id = client_id
        if client_id is None:
            pad = "%032x" % random.getrandbits(128)
            _id = 'weewx_%s' % pad[:8]
        self.client_id = _id
        # obtain a dict of TLS settings if provided
        self.tls_dict = {}
        if tls is not None:
            # we have TLS options so construct a dict to configure Paho TLS
            dflts = TLSDefaults()
            for opt in tls:
                if opt == 'cert_reqs':
                    if tls[opt] in dflts.CERT_REQ_OPTIONS:
                        self.tls_dict[opt] = dflts.CERT_REQ_OPTIONS.get(tls[opt])
                elif opt == 'tls_version':
                    if tls[opt] in dflts.TLS_VER_OPTIONS:
                        self.tls_dict[opt] = dflts.TLS_VER_OPTIONS.get(tls[opt])
                elif opt in dflts.TLS_OPTIONS:
                    self.tls_dict[opt] = tls[opt]
            # log the TLS if debug >= 1 or mqtt_debug > 0
            if weewx.debug > 0 or self.mqtt_debug > 0:
                loginf("TLS parameters: %s" % self.tls_dict)

        self.retain = retain
        # MQTT quality of service
        self.qos = qos
        # timeout in seconds when connecting to the MQTT broker
        self.timeout = timeout
        # max number of time to attempt connection/publication
        self.max_tries = max_tries
        # time in seconds to wait between attempts to connect/publish
        self.retry_wait = retry_wait
        # whether to log successful publication
        self.log_success = log_success
        # provide extra MQTT debug information if > 0
        self.mqtt_debug = mqtt_debug
        # log the mqtt_debug setting
        loginf("mqtt_debug is %d" % self.mqtt_debug)
        # create a placeholder for our paho MQTT client
        self.mqtt_client = None

    def connect(self):
        """Connect to a MQTT broker.

        Connect to a broker with a client, or if a previous connection/client
        exists attempt to reconnect. If reconnection is unsuccessful (eg the
        MQTT broker daemon has been restarted) obtain a new client and connect
        to the broker. A successful connection results in the mqtt_client property
        being set to the Client object.

        If connection fails due to a socket error a ConnectionError exception
        is raised with an appropriate error message. If a connection error
        message is received from the on_connect callback a ConnectionError
        exception is raised with the received connection error message.
        Otherwise if the on_connect callback does not respond with a successful
        connection within the timeout period a ConnectionError is raised.
        """

        # do we have a client object
        if self.mqtt_client is not None:
            # We have a client object but are we connected? If not connected
            # then attempt to reconnect, otherwise continue.
            if not self.mqtt_client.connected:
                # wrap the reconnect in a try block to catch any socket errors
                try:
                    self.mqtt_client.reconnect()
                except (socket.error, socket.timeout, socket.herror) as e:
                    loginf("socket error: %s" % (e,))
                else:
                    # was the reconnect successful
                    if self.mqtt_client.connected:
                        # start the client loop
                        self.mqtt_client.loop_start()
                    else:
                        # Could not reconnect, perhaps the broker has restarted.
                        # Get a new client
                        if weewx.debug >= 3 or self.mqtt_debug > 0:
                            loginf("Could not reconnect to MQTT broker, creating new client...")
                        # get a new client
                        self.mqtt_client = self.get_client()
                        if weewx.debug >= 3 or self.mqtt_debug > 0:
                            loginf("New MQTT client created")
        else:
            # we don't have a client so we need to create one
            self.mqtt_client = self.get_client()

    def get_client(self):
        """Create a MQTT client and connect to the broker."""

        # first get a Paho client object
        _client = mqtt.Client(self.client_id)
        # setup the client callbacks
        _client.on_connect = self.on_connect
        _client.on_disconnect = self.on_disconnect
        _client.on_publish = self.on_publish
        # initialise some flags
        # connected flag, True if we are connected to our broker
        _client.connected = False
        # decoded error paho connection message
        _client.conn_error_msg = None
        # parse the MQTT server URL
        url = urlparse(self.server_url)
        # if we have a user name and password supplied use them
        if url.username is not None and url.password is not None:
            _client.username_pw_set(url.username, url.password)
        # if we have TLS options configure TLS on our broker connection
        if len(self.tls_dict) > 0:
            _client.tls_set(**self.tls_dict)
        # get a timestamp to base any timeout on
        _start_ts = time.time()
        # connect to the broker, wrap in a try..except in case we have a
        # socket error
        try:
            # connect to the MQTT broker
            _client.connect(url.hostname, url.port)
        except (socket.error, socket.timeout, socket.herror) as e:
            raise ConnectionError("socket error: %s" % (e,))
        # start the network loop so our callbacks can react
        _client.loop_start()
        # Wait and see if we have a successful connection. If we do the
        # on_connect callback will set the connected flag and we can
        # continue. If we don't get a connection after our timeout then
        # raise an exception.
        while not _client.connected:
            # if we have timed out then raise
            if time.time() > self.timeout + _start_ts:
                raise ConnectionError("Connection timeout")
            # if we have received a connection error message then raise
            if _client.conn_error_msg:
                raise ConnectionError(_client.conn_error_msg)
            # otherwise sleep
            time.sleep(0.1)
        # if we made it this far we have a successful connection
        return _client

    def disconnect(self):
        """Disconnect from a previously connected MQTT broker."""

        # disconnect if we have a client that has been connected
        if self.mqtt_client:
            # call the paho client disconnect method
            self.mqtt_client.disconnect()
            # not sure if you could get a timeout on a disconnection but will
            # cover it in any case
            # get a timestamp to base any timeout on
            _start_ts = time.time()
            # Wait and see if we have a successful disconnection. If we do the
            # on_disconnect callback will reset the connected flag and we can
            # continue. If we don't get a connection after our timeout then
            # raise an exception.
            while self.mqtt_client.connected:
                if time.time() > self.timeout + _start_ts:
                    raise ConnectionError("Disconnection timeout")
                time.sleep(0.1)

    def publish(self, topic, data, identifier, qos=2):
        """Publish data to a MQTT broker.

        Publishes data to topic. Publish failures (socket.error, socket.timeout
        and socket.herror) that trigger a retry are logged as are any other
        failures reported by Paho.

        Parameters:
            topic:      Topic to which the data is to be published
            data:       Data to be published
            identifier: Identifier (eg timestamp) used to identify a particular
                        publication in the system logs (eg successful
                        publishing)
            qos:        MQTT quality of service level to be used when
                        publishing
        """

        # reset published flag
        self.mqtt_client.published = False
        # try to publish self.max_tries times
        for _count in range(self.max_tries):
            # publish the message, wrap in try..except to catch any socket errors
            try:
                (res, mid) = self.mqtt_client.publish(topic,
                                                      payload=data,
                                                      qos=qos,
                                                      retain=self.retain)
            except (socket.error, socket.timeout, socket.herror) as e:
                # we encountered a network error, log it and continue to the
                # next attempt
                if weewx.debug >= 3 or self.mqtt_debug > 0:
                    loginf("Failed publish attempt %d for '%s': %s" % (_count + 1,
                                                                       topic,
                                                                       e))
            else:
                # no network error but that doesn't mean publication was
                # successful, wait self.timeout seconds for success
                _start_ts = time.time()
                while not self.mqtt_client.published:
                    if time.time() > self.timeout + _start_ts:
                        if weewx.debug >= 3 or self.mqtt_debug > 0:
                            loginf("Could not publish attempt %d for %s: timeout" % (_count + 1,
                                                                                     topic))
                        if _count < self.max_tries - 1:
                            # we still have more attempts so sleep until the
                            # next attempt is due
                            time.sleep(self.retry_wait)
                        # and continue
                        continue
                    time.sleep(0.1)
                # so the client thinks publication was a success, but what what
                # was the actual result in res
                if res != mqtt.MQTT_ERR_SUCCESS:
                    # res is something other than success, this should not
                    # happen but log it anyway
                    logerr("Failed publish attempt %d for '%s': %s (%d)" % (_count + 1,
                                                                            topic,
                                                                            mqtt.error_string(res),
                                                                            res))
                    if _count < self.max_tries - 1:
                        # we still have more attempts so sleep until the
                        # next attempt is due
                        time.sleep(self.retry_wait)
                    continue
                elif self.log_success or self.mqtt_debug > 0:
                    # publication was a success, we need to log that if we are
                    # logging success so or if debug=2
                    loginf("Published data(%s) for '%s'" % (identifier,
                                                            topic))
                # we are done so we can return
                return
        else:
            # we exhausted our attempts, log it and continue
            raise FailedPost("Failed to publish data(%s) for %s after %d tries" %
                             (identifier, topic, self.max_tries))

    def on_connect(self, client, userdata, flags, rc):
        """Paho on_connect callback.

        Retrieves the CONNACK message. If the connection was accepted
        (CONNACK_ACCEPTED) the connection is logged and the connection error
        message property set to None. Otherwise the connection error message
        property contains the decoded CONNACK message.

        Parameters:
            client:   The Paho client instance for this callback
            userdata: The private user data as set in Client() or
                      userdata_set()
            flags:    Dict containing response flags from the broker
            rc:       Response code determining success or otherwise
        """

        # get the connection message
        conn_msg = mqtt.connack_string(rc)
        # was the connection was accepted ?
        if rc == mqtt.CONNACK_ACCEPTED:
            # successful connection, log the result and set some flags
            if weewx.debug >= 3 or self.mqtt_debug > 0:
                loginf(conn_msg)
            # flag indicating we have a connection to the broker
            client.connected = True
            # reset the connection error message
            client.conn_error_msg = None
        else:
            # unsuccessful connection, log the result and store the error
            # message
            if weewx.debug > 0 or self.mqtt_debug > 0:
                loginf(conn_msg)
            # store the error message
            client.conn_error_msg = conn_msg

    def on_disconnect(self, client, userdata, rc):
        """Paho on_disconnect callback.

        Log the disconnection, reset the connected flag and stop the network
        loop. If the disconnection was not due to a Client.disconnect call
        store an error message.
        """

        # log the disconnection
        if weewx.debug >= 3 or self.mqtt_debug > 0:
            loginf('Disconnected with rtn code="%d"' % (rc,))
        # reset the connected flag
        client.connected = False
        # if the disconnection was not the result of a disconnect() call then
        # it is unexpected and may indicate a network error.
        if rc != mqtt.MQTT_ERR_SUCCESS:
            client.conn_error_msg = "Unexpected client disconnection. Possible network error."
        # stop the network loop
        client.loop_stop()

    def on_publish(self, client, userdata, mid):
        """Paho on_publish callback.

        Log the publication and set the published flag.
        """

        # log the publication
        if weewx.debug >= 3 or self.mqtt_debug > 0:
            loginf('Published with message ID="%d"' % (mid,))
        # set the published flag
        client.published = True


# ============================================================================
#                             class TLSDefaults
# ============================================================================

class TLSDefaults(object):
    """Class used to construct a dict of TLS default options."""

    def __init__(self):
        import ssl

        # Paho acceptable TLS options
        self.TLS_OPTIONS = [
            'ca_certs', 'certfile', 'keyfile',
            'cert_reqs', 'tls_version', 'ciphers'
            ]
        # map for Paho acceptable TLS cert request options
        self.CERT_REQ_OPTIONS = {
            'none': ssl.CERT_NONE,
            'optional': ssl.CERT_OPTIONAL,
            'required': ssl.CERT_REQUIRED
            }
        # Map for Paho acceptable TLS version options. Some options are
        # dependent on the OpenSSL install so catch exceptions
        self.TLS_VER_OPTIONS = dict()
        try:
            self.TLS_VER_OPTIONS['tls'] = ssl.PROTOCOL_TLS
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            self.TLS_VER_OPTIONS['tlsv1'] = ssl.PROTOCOL_TLSv1
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            self.TLS_VER_OPTIONS['tlsv11'] = ssl.PROTOCOL_TLSv1_1
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead if python >= 2.7.13
            self.TLS_VER_OPTIONS['tlsv12'] = ssl.PROTOCOL_TLSv1_2
        except AttributeError:
            pass
        try:
            # SSLv2 is insecure - this protocol is deprecated
            self.TLS_VER_OPTIONS['sslv2'] = ssl.PROTOCOL_SSLv2
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            # (alias for PROTOCOL_TLS)
            self.TLS_VER_OPTIONS['sslv23'] = ssl.PROTOCOL_SSLv23
        except AttributeError:
            pass
        try:
            # SSLv3 is insecure - this protocol is deprecated
            self.TLS_VER_OPTIONS['sslv3'] = ssl.PROTOCOL_SSLv3
        except AttributeError:
            pass


# ============================================================================
#                               class DayCache
# ============================================================================

class DayCache(object):
    """Dictionary of value-timestamp pairs.  Each timestamp indicates when the
    corresponding value was last updated."""

    def __init__(self):
        self.unit_system = None
        self.values = dict()
        self.maximums = dict()
        self.minimums = dict()

    def reset(self):
        """Reset the maximums and minimums.

        To reset the maximums and minimums we simply reset self.maximums and
        self.minimums to empty dicts.
        """

        self.maximums = dict()
        self.minimums = dict()

    def update(self, packet, ts):
        # update the cache with values from the specified packet, using the
        # specified timestamp.
        for k in packet:
            if k is None:
                # well-formed packets do not have None as key, but just in case
                continue
            elif k == 'dateTime':
                # do not cache the timestamp
                continue
            elif k == 'usUnits':
                # assume unit system of first packet, then enforce consistency
                if self.unit_system is None:
                    self.unit_system = packet[k]
                elif packet[k] is None:
                    raise ValueError("Invalid unit system encountered in cache: 'usUnits'=%s"
                                     % (packet[k],))
                elif packet[k] != self.unit_system:
                    raise ValueError("Mixed units encountered in cache. %s vs %s"
                                     % (self.unit_system, packet[k]))
            else:
                # cache each value, associating it with the time it was cached
                self.values[k] = {'value': packet[k], 'ts': ts}
                if packet[k] is not None:
                    if k not in self.maximums or ('max' in self.maximums[k] and packet[k] > self.maximums[k]['max']):
                        self.maximums[k] = {'max': packet[k], 'max_ts': ts}
                    if k not in self.minimums or ('min' in self.minimums[k] and packet[k] < self.minimums[k]['min']):
                        self.minimums[k] = {'min': packet[k], 'min_ts': ts}

    def get_value(self, k, ts, stale_age):
        """Get a value for a given key from the cache.

        Get the value for the specified key. If the value is older than
        stale_age (seconds) then return None.
        """

        if k in self.values and ts - self.values[k]['ts'] < stale_age:
            return self.values[k]['value']
        return None

    def get_maximum(self, k):
        """Get the maximum for a given observation.

        Get the maximum value and timestamp the value was seen for the
        specified key. If the key does not exist return None.
        """

        return self.maximums.get(k, {'max': None, 'max_ts': None})

    def get_minimum(self, k):
        """Get the minimum for a given observation.

        Get the minimum value and timestamp the value was seen for the
        specified key. If the key does not exist return None.
        """

        return self.minimums.get(k, {'min': None, 'min_ts': None})

    def get_max_min_packet(self, ts=None, stale_age=960):
        """Get a cached packet that includes max/min values and timestamps."""

        if ts is None:
            ts = int(time.time() + 0.5)
        pkt = {'dateTime': ts, 'usUnits': self.unit_system}
        for k in self.values:
            pkt[k] = dict()
            pkt[k]['now'] = self.get_value(k, ts, stale_age)
            pkt[k]['today'] = self.get_maximum(k)
            pkt[k]['today'].update(self.get_minimum(k))
        return pkt

    def get_packet(self, ts=None, stale_age=960):
        if ts is None:
            ts = int(time.time() + 0.5)
        pkt = {'dateTime': ts, 'usUnits': self.unit_system}
        for k in self.values:
            pkt[k] = self.get_value(k, ts, stale_age)
        return pkt


# ============================================================================
#                             Utility functions
# ============================================================================

def obfuscate_password(url, o_str='xxx'):
    """Obfuscate the 'password' portion of a URL.

    Splits the URL into it's six component parts and then obfuscates the
    Password sub-component of the netloc component. The URL is re-assembled
    from the component parts and returned with the password obfuscated.
    """

    # just in case I was passed None for o_str
    o_str = 'xxx' if o_str is None else o_str
    # obtain the URL component parts
    parts = urlparse(url)
    # if we have a password then obfuscate it
    if parts.password is not None:
        # Split out the host portion manually. We could use
        # parts.hostname and parts.port, but then you'd have to check
        # if either part is None. The hostname would also be lowercase.
        host_info = parts.netloc.rpartition('@')[-1]
        # now reconstruct the netloc and use the new netloc in our tuple of
        # component parts
        parts = parts._replace(netloc='{}:{}@{}'.format(parts.username,
                                                        o_str,
                                                        host_info))
        # re-assemble the URL
        url = parts.geturl()
    return url


def obfuscate_client(url, obf_ch='x'):
    """Obfuscate the client_id and client_secret query parameters in a URL.

    Splits the URL into it's six component parts and then obfuscates the
    client_id and client_secret parameters in the query component. The URL is
    re-assembled from the component parts and returned with the obfuscated
    parameters.
    """

    # obtain the URL component parts
    url_parts = urlparse(url)
    # do we have any query parameters
    if len(url_parts[4]) > 0:
        # now parse the query string
        q_dict = parse_qs(url_parts[4])
        # we only need to do something if we have a client_id or a
        # client_secret query parameter
        if 'client_id' in q_dict or 'client_secret' in q_dict:
            if 'client_id' in q_dict:
                # replace the client_id value with an obfuscated version
                q_dict['client_id'] = ''.join([obf_ch * (len(q_dict['client_id'][0]) - 4),
                                               q_dict['client_id'][0][-4:]])
            if 'client_secret' in q_dict:
                # replace the client_secret value with an obfuscated version
                q_dict['client_secret'] = ''.join([obf_ch * (len(q_dict['client_secret'][0]) - 4),
                                                   q_dict['client_secret'][0][-4:]])
            # replace the query tuple in the decomposed url with the obfuscated
            # version
            url_parts = url_parts._replace(query=urlencode(q_dict))
            # return then re-assemble the URL
            return url_parts.geturl()
        else:
            # we had query parameters but noe we had to change so return the
            # original url
            return url
    else:
        # there is no query parameters so return the original url
        return url
