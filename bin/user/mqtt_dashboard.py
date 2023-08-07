"""
mqtt_dashboard.py

A collection of WeeWX services for publishing to my MQTT broker.

Portions based on the MQTT uploader v0.23 Copyright 2013-2021 Matthew Wall and
distributed under the terms of the GNU Public License (GPLv3).

Copyright (C) 2021-2023 Gary Roderick                gjroderick<at>gmail.com

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see http://www.gnu.org/licenses/.

Version: 0.2.0                                      Date: 30 May 2023

Revision History
    30 May 2023         v0.2.0
        - reworked MQTT client reconnection logic
    9 March 2021        v0.1.0
        -   initial release

This file contains the class definitions for the following WeeWX services:

MQTTDashboardLoop:  a RESTful service for publishing WeeWX loop data to a MQTT
                    broker

MqttDashboardAeris: a service for obtaining and publishing Aeris forecast and
                    conditions data to a MQTT broker

MqttDashboardSlow:  a service for publishing slow changing observation (usually
                    aggregate) data to a MQTT broker every archive period

Supporting class definitions include:

MqttPublisher: a class for publishing data to a MQTT broker

TLSDefaults:   a class used to construct default TLS options for use when using
               TLS to connect to a MQTT broker

DayCache:      a specialist cache of day based observation data
"""
# Python imports
import datetime
import json
import math
import operator
import paho.mqtt.client as mqtt
import random
import socket
import threading
import time
from operator import itemgetter

# Python 2/3 imports in different libraries
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
import weeutil.weeutil
import weewx
import weewx.almanac
import weewx.engine
import weewx.manager
import weewx.restx
import weewx.tags
import weewx.xtypes
from weeutil.config import conditional_merge
from weewx.units import ValueTuple
from weeutil.weeutil import to_bool, to_float, to_int, timestamp_to_string, to_sorted_string

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

# length of history to be maintained in seconds
MAX_AGE = 600
# list of obs that we will attempt to buffer
MANIFEST = ['outTemp', 'barometer', 'outHumidity', 'rain', 'rainRate',
            'humidex', 'windchill', 'heatindex', 'windSpeed', 'inTemp',
            'appTemp', 'dewpoint', 'windDir', 'UV', 'radiation', 'wind',
            'windGust', 'windGustDir', 'windrun']
# obs for which we need a running sum
SUM_MANIFEST = ['rain']
# obs for which we need a history
HIST_MANIFEST = ['windSpeed', 'windDir', 'windGust', 'wind']


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
#                         class MqttDashboardService
# ============================================================================

class MqttDashboardService(weewx.engine.StdService):
    """Base class for a WeeWX service that publishes data to a MQTT broker.

    The derived class normally uses a threaded object to deal with the MQTT
    broker.

    The base class creates a Queue object for passing loop packets and archive
    records to its subordinate thread.

    Includes methods for placing loop packets and archive records in the Queue,
    but does not include the required method bindings. Any binding should be
    added to the derived classes as required.

    The shutDown method passes None via the Queue to the subordinate thread as
    a signal to shut down.
    """

    def __init__(self, engine, config_dict):
        # initialize my superclass
        super(MqttDashboardService, self).__init__(engine, config_dict)

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
        if self.mqtt_debug >= 3:
            loginf("Queued archive record: %s %s" % (timestamp_to_string(event.record['dateTime']),
                                                     to_sorted_string(event.record)))
        elif self.mqtt_debug == 2:
            loginf("Queued archive record dateTime=%s" % timestamp_to_string(event.record['dateTime']))

    def new_loop_packet(self, event):
        """Puts loop packets in the queue."""

        self.queue.put(event.packet)
        if self.mqtt_debug >= 3:
            loginf("Queued loop packet: %s %s" % (timestamp_to_string(event.packet['dateTime']),
                                                  to_sorted_string(event.packet)))
        elif self.mqtt_debug == 2:
            loginf("Queued loop packet dateTime=%s" % timestamp_to_string(event.packet['dateTime']))

    def shutDown(self):
        """Shut down any threads."""

        if hasattr(self, 'queue') and hasattr(self, 'thread'):
            if self.queue and self.thread.is_alive():
                # put a None in the queue to signal the thread to shut down
                self.queue.put(None)
                # wait up to 20 seconds for the thread to exit:
                self.thread.join(20.0)
                if self.thread.is_alive():
                    logerr("Unable to shut down '%s' thread" % self.thread.name)
                else:
                    logdbg("Shut down '%s' thread" % self.thread.name)


# ============================================================================
#                        class MqttDashboardRealtime
# ============================================================================

class MqttDashboardRealtime(MqttDashboardService):
    """Service that publishes loop based data to a MQTT broker."""

    version = '0.2.0'

    def __init__(self, engine, config_dict):

        # first up say what we are loading
        loginf("Loading service MqttDashboardRealtime v%s" % MqttDashboardRealtime.version)

        # get my config dict
        try:
            rt_config_dict = weeutil.config.accumulateLeaves(config_dict['MQTTDashboard']['Realtime'],
                                                             max_level=1)
        except KeyError:
            logerr("Service MqttDashboardRealtime not loaded: Unable to find [MQTTDashboard] [[Realtime]] config stanza")
            return
        # we have our config dict, now get my broker config dict
        broker_config_dict = get_mqtt_broker_dict(config_dict=config_dict,
                                                  service='Realtime',
                                                  service_class_name='MqttDashboardRealtime',
                                                  reqd_options=['server_url', 'topic'])
        # if our broker config dict is None we are unable to continue
        if broker_config_dict is None:
            return

        # now initialize my superclass
        super(MqttDashboardRealtime, self).__init__(engine, config_dict)

        # get a manager config dict, we need this to access the database
        manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                  'wx_binding')
        # get our mqtt_debug level, if we don't have such a config option use 0
        mqtt_debug = rt_config_dict.get('mqtt_debug', 0)
        # get an instance of class MqttDashboardRealtimeThread and start the
        # thread running
        self.thread = MqttDashboardRealtimeThread(queue=self.queue,
                                                  rt_config_dict=rt_config_dict,
                                                  broker_config_dict=broker_config_dict,
                                                  manager_dict=manager_dict,
                                                  mqtt_debug=mqtt_debug)
        self.thread.start()

        # bind our self to the WeeWX NEW_LOOP_PACKET event
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        # bind our self to the WeeWX NEW_ARCHIVE_RECORD event
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)


# ============================================================================
#                     class MqttDashboardRealtimeThread
# ============================================================================

class MqttDashboardRealtimeThread(threading.Thread):
    """Thread to publish loop based data to a MQTT broker."""

    def __init__(self, queue, rt_config_dict, broker_config_dict, manager_dict, mqtt_debug):

        # initialize my superclass
        threading.Thread.__init__(self)

        # set my thread name and run as a daemon
        self.setName('MqttDashboardRealtimeThread')
        self.setDaemon(True)
        # save our queue for receiving data
        self.queue = queue
        # save a manager dict so we can access the database
        self.manager_dict = manager_dict

        # MQTT broker URL
        server_url = broker_config_dict.get('server_url')
        # topic to publish to
        self.topic = broker_config_dict.get('topic', 'weather/realtime')
        # TLS options
        tls_opt = broker_config_dict.get('tls', None)
        # quality of service
        qos = to_int(broker_config_dict.get('tls', 0))
        # will MQTT broker retain messages
        retain = to_bool(broker_config_dict.get('retain', True))
        # log successful publishing of data
        log_success = to_bool(broker_config_dict.get('log_success', False))
        # save our mqtt_debug level
        self.mqtt_debug = mqtt_debug

        # the fields we are to include in our output
        self.inputs = rt_config_dict.get('inputs', {})

        # do our config logging before the Publisher
        loginf("%s: Data will be published to '%s' under topic '%s'" % (self.name,
                                                                        obfuscate_password(server_url),
                                                                        self.topic))
        loginf("%s: qos is %d, retain is %r, log success is %s" % (self.name,
                                                                   qos,
                                                                   retain,
                                                                   log_success))

        # get a MQTTPublisher object to do the publishing for us
        self.publisher = MqttPublisher(server_url=server_url,
                                       tls=tls_opt,
                                       retain=retain,
                                       qos=qos,
                                       log_success=log_success,
                                       mqtt_debug=mqtt_debug)

        # initialise some properties we will use
        # TimeSpan for the current day
        self.day_span = None
        # is it a new day, used for 9am resets
        self.new_day = False
        # db manager
        self.db_manager = None
        # stats unit system
        self.stats_unit_system = None
        # Buffer object
        self.buffer = None

    def run(self):
        """Collect data from the queue and manage its processing."""

        # get a db manager
        self.db_manager = weewx.manager.open_manager(self.manager_dict)
        # obtain the current day stats so we can initialise a Buffer object
        day_stats = self.db_manager._get_day_summary(time.time())
        # save our day stats unit system for use later
        self.stats_unit_system = day_stats.unit_system
        # get a Buffer object
        self.buffer = Buffer(MANIFEST, day_stats=day_stats)
        # wrap our main processing loop in a try..except, if any exceptions are
        # raised we want to be able to capture them and provide feedback rather
        # than the thread just silently dying
        try:
            # Run a continuous loop, processing data received in the queue. Only
            # break out if we receive the shutdown signal (None) from our parent.
            while True:
                # Run an inner loop checking for the shutdown signal and keeping
                # the queue length from getting too long. If a loop packet is
                # received break out of the loop and process it
                while True:
                    _package = self.queue.get()
                    if _package is None:
                        # None is our signal to exit, disconnect cleanly if we
                        # have already connected then return
                        if self.publisher:
                            self.publisher.disconnect()
                        return
                    # if packets have backed up in the queue, trim it until it's no
                    # bigger than the max allowed backlog
                    if self.queue.qsize() <= 5:
                        break

                # we now have a packet to process
                # First, log receipt. The amount of logging depends on the debug
                # level.
                if self.mqtt_debug >= 3:
                    loginf("Received loop packet: %s %s" % (timestamp_to_string(_package['dateTime']),
                                                            to_sorted_string(_package)))
                elif self.mqtt_debug == 2:
                    loginf("Received loop packet dateTime=%s" % timestamp_to_string(_package['dateTime']))
                # and finally process the packet
                self.process_packet(_package)
        except Exception as e:
            # Some unknown exception occurred. This is probably a serious
            # problem. Log and exit.
            logcrit("Unexpected exception of type %s" % (type(e), ))
            log_traceback_critical('mqttdashboardrealtimethread: **** ')
            logcrit("Thread exiting. Reason: %s" % (e, ))

    def process_packet(self, packet):
        """Process a received loop packet."""

        # if we have the first packet from a new day we need to reset the Buffer
        # objects stats
        if self.day_span is not None:
            # we have a day_span so this is not our first time, check to see if
            # our packet timestamp belongs to the following day
            if packet['dateTime'] > self.day_span.stop:
                # we have a packet from a new day, so reset the Buffer stats
                # TODO. Development code only, remove statement before release
                if self.mqtt_debug >= 1:
                    loginf("Start of day, resetting buffer[rain].sum. buffer[rain].sum=%s" % (self.buffer['rain'].sum,))
                self.buffer.start_of_day_reset()
                # TODO. Development code only, remove statement before release
                if self.mqtt_debug >= 1:
                    loginf("Buffer reset. buffer[rain].sum=%s" % (self.buffer['rain'].sum,))
                # and reset our day_span
                self.day_span = weeutil.weeutil.archiveDaySpan(packet['dateTime'])
                # and set the new_day property to True
                self.new_day = True
            # If this is the first packet after 9am we need to reset any 9am
            # sums. There is a bit a dilemma here, a packet timestamped at 9am
            # actually has data from the period before 9am, so if we reset when
            # the packet is timestamped 9am we might add data to our buffer
            # that was from before 9am. But if we reset on the first packet
            # after 9am we may be displaying yesterday's rain from 9am until
            # the next packet comes in. Since loop packets usually come in very
            # regularly we will choose to reset on the first packet after 9am.

            # first get the hour and minutes of the packet timestamp as an ints
            _hms = time.strftime('%H:%M:%S', time.localtime(packet['dateTime']))
            _h, _m, _s = [int(x) for x in _hms.split(':')]
            # if it's a new day and hour >= 9 and minute > 0 or second > 0 we need
            # to reset any 9am sums
            if self.new_day and _h >= 9 and (_m > 0 or _s > 0):
                # TODO. Development code only, remove statement before release
                if self.mqtt_debug >= 1:
                    loginf("9am resetting buffer. buffer[rain].nineam_sum=%s" % (self.buffer['rain'].nineam_sum,))
                self.new_day = False
                self.buffer.nineam_reset()
                # TODO. Development code only, remove statement before release
                if self.mqtt_debug >= 1:
                    loginf("9am buffer reset. buffer[rain].nineam_sum=%s" % (self.buffer['rain'].nineam_sum,))
        else:
            # we don't have a day_span, it must be the first packet since we
            # started, so initialise a day_span
            self.day_span = weeutil.weeutil.archiveDaySpan(packet['dateTime'])
            # now that we have our first packet we can initialise the 9am rain
            # sum in our Buffer object, we couldn't do it earlier as we did not
            # know which 9am to use, yesterdays or todays
            self.buffer['rain'].nineam_sum = self.get_nineam_rain(packet['dateTime'])
            if self.mqtt_debug >= 1:
                loginf("setting 9am rain=%s" % (self.buffer['rain'].nineam_sum,))

        # convert our incoming packet
        _conv_packet = weewx.units.to_std_system(packet,
                                                 self.stats_unit_system)
        # update the buffer with the converted packet
        self.buffer.add_packet(_conv_packet)
        # obtain the message we are to publish
        msg = self.generate_message(_conv_packet)
        # connect to our mqtt broker
        self.publisher.connect()
        # publish to MQTT broker
        self.publisher.publish(self.topic,
                               json.dumps(msg),
                               _conv_packet.get('dateTime', int(time.time())))

    def generate_message(self, packet):
        """Generate the message to be published.

        Creates and returns a dict containing the data to be published.
        """

        data = {'dateTime': packet['dateTime'],
                'usUnits': packet['usUnits']}
        # iterate over each of the keys in our input config, if that key is
        # in the record then add the record field to our dict, if the key is
        # not see if there is anything in our buffer we can use
        for f in self.inputs.keys():
            if f in packet:
                # don't add dateTime, it's already there and is mandatory anyway
                if f == 'dateTime':
                    continue
                # add the current (or now) value unless we have been explicitly
                # told not to
                if to_bool(self.inputs[f].get('now', True)):
                    # add a field placeholder in our data dict
                    data[f] = dict()
                    # and add the current value
                    data[f]['now'] = packet[f]
                # now handle any aggregates
                aggs = weeutil.weeutil.option_as_list(self.inputs[f].get('agg', []))
                if len(aggs) > 0:
                    # add a field placeholder in our data dict if we have not
                    # already done so
                    if f not in data:
                        data[f] = dict()
                    # add a placeholder for our aggregates
                    data[f]['today'] = dict()
                    # and add each aggregate
                    for agg in aggs:
                        # wrap in a try..except in case we find an aggregate we
                        # don't know about
                        try:
                            data[f]['today'][agg] = getattr(self.buffer[f], agg)
                        except AttributeError:
                            # we couldn't find that attribute so skip it
                            pass
            elif f in self.buffer:
                aggs = weeutil.weeutil.option_as_list(self.inputs[f].get('agg', []))
                if len(aggs) > 0:
                    # add a field placeholder in our data dict
                    data[f] = dict()
                    # add a placeholder for our aggregates
                    data[f]['today'] = dict()
                    # and add each aggregate
                    for agg in aggs:
                        # wrap in a try..except in case we find an aggregate we
                        # don't know about
                        try:
                            data[f]['today'][agg] = getattr(self.buffer[f], agg)
                        except AttributeError:
                            # we couldn't find that attribute so skip it
                            pass

        # return our data
        return data

    def get_nineam_rain(self, tstamp):
        """Get the rainfall since 9am.

        Given a timestamp get the rainfall from 9am before the timestamp
        through until the timestamp.

        Returns a value in the units used by the Buffer object. If the value
        could not be calculated 0.0 is returned.
        """

        # First get a timestamp for the last 9am, but is it today or yesterday
        # get datetime obj for the time of our packet
        today_dt = datetime.datetime.fromtimestamp(tstamp)
        # now get time obj for midnight
        midnight_t = datetime.time(0)
        # get datetime obj for midnight at start of today
        midnight_dt = datetime.datetime.combine(today_dt, midnight_t)
        # get the current hour
        _h = int(time.strftime('%H', time.localtime(tstamp)))
        # if it is after 9am add 9 hours to our
        if _h >= 9:
            # we want the timestamp at 9am today so add nine hours
            nineam_dt = midnight_dt + datetime.timedelta(hours=9)
        else:
            # we want the timestamp at 9am yesterday so subtract 15 hours
            nineam_dt = midnight_dt - datetime.timedelta(hours=15)
        # get it as a timestamp
        nineam_ts = time.mktime(nineam_dt.timetuple())
        nineam_tspan = weeutil.weeutil.TimeSpan(nineam_ts, tstamp)
        try:
            rain_vt = weewx.xtypes.get_aggregate(obs_type='rain',
                                                 timespan=nineam_tspan,
                                                 aggregate_type='sum',
                                                 db_manager=self.db_manager)
        except (weewx.UnknownType,
                weewx.UnknownAggregation,
                weewx.CannotCalculate):
            return 0.0
        else:
            return weewx.units.convertStd(rain_vt, self.stats_unit_system).value


# ============================================================================
#                          class MqttDashboardAeris
# ============================================================================

class MqttDashboardAeris(MqttDashboardService):
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

    version = '0.2.0'

    def __init__(self, engine, config_dict):

        # first up say what we are loading
        loginf("Loading service MqttDashboardAeris v%s" % MqttDashboardAeris.version)

        # get my config dict
        try:
            aeris_config_dict = weeutil.config.accumulateLeaves(config_dict['MQTTDashboard']['Aeris'],
                                                                max_level=1)
        except KeyError:
            logerr("Service MqttDashboardAeris not loaded: Unable to find [MQTTDashboard] [[Aeris]] config stanza")
            return
        # we have our config dict, now get my broker config dict
        broker_config_dict = get_mqtt_broker_dict(config_dict=config_dict,
                                                  service='Aeris',
                                                  service_class_name='MqttDashboardAeris',
                                                  reqd_options=['client_id', 'client_secret'])
        # if our broker config dict is None we are unable to continue
        if broker_config_dict is None:
            return

        # initialize my superclass
        super(MqttDashboardAeris, self).__init__(engine, config_dict)

        # get our mqtt_debug level, if we don't have such a config option use 0
        mqtt_debug = aeris_config_dict.get('mqtt_debug', 0)
        # get an instance of class MqttDashboardAerisThread and start the
        # thread running
        alt_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter').value
        self.thread = MqttDashboardAerisThread(self.queue,
                                               aeris_config_dict,
                                               broker_config_dict,
                                               lat=engine.stn_info.latitude_f,
                                               long=engine.stn_info.longitude_f,
                                               alt_m=alt_m,
                                               mqtt_debug=mqtt_debug)
        self.thread.start()


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

    def __init__(self, queue, aeris_config_dict, mqtt_config_dict, lat, long, alt_m, mqtt_debug):

        # Initialize my superclass
        threading.Thread.__init__(self)

        # set my thread name and run as a daemon
        self.setName('MqttDashboardAerisThread')
        self.setDaemon(True)
        # save our queue for receiving data
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
        # save our mqtt_debug level
        self.mqtt_debug = mqtt_debug

        # do our config logging before the Publisher
        loginf("%s: Data will be published to '%s'" % (self.name,
                                                       obfuscate_password(server_url)))
        loginf("%s: Conditions will be published to topic '%s'" % (self.name,
                                                                   self.topic['observations']))
        loginf("%s: Forecast will be published to topic '%s'" % (self.name,
                                                                 self.topic['forecasts']))
        loginf("%s: qos is %d, retain is %r, log success is %s" % (self.name,
                                                                   qos,
                                                                   retain,
                                                                   log_success))

        # get a MQTTPublisher object to do the publishing for us
        self.publisher = MqttPublisher(server_url=server_url,
                                       tls=tls_opt,
                                       retain=retain,
                                       qos=qos,
                                       log_success=log_success,
                                       mqtt_debug=mqtt_debug)

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
        # both don't exist).
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
                # If we haven't made this API call previously or if it has been
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
                         location for which the data is required. String.
            parameters:  Optional parameters to be included in the API call
                         eg filter to filter to limit the results. Dictionary
                         with elements in the format parameter: value.
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
                        loginf("An error was encountered in the API "
                               "response: %s" % (response_json['error']['description']))
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

class MqttDashboardSlow(MqttDashboardService):
    """Service that publishes slow changing JSON format data to an MQTT broker.

    The MQTTDashboardSlow class creates and controls a threaded object of class
    MQTTDashboardSlowThread that generates slow changing JSON data once each
    archive period and publishes this data to a MQTT broker.

    MQTTDashboardSlow constructor parameters:

        engine:      a WeeWX engine, usually an instance of
                     weewx.engine.StdEngine
        config_dict: a WeeWX config dictionary

    MQTTDashboardSlow methods:

        new_archive_record. Action to be taken upon receipt of a new archive
                            record.
        ShutDown.           Shutdown any child threads.
    """
    version = '0.2.0'

    def __init__(self, engine, config_dict):

        # first up say what we are loading
        loginf("Loading service MqttDashboardSlow v%s" % MqttDashboardSlow.version)

        # get my config dict
        try:
            slow_config_dict = weeutil.config.accumulateLeaves(config_dict['MQTTDashboard']['Slow'],
                                                               max_level=1)
        except KeyError:
            logerr("Service MqttDashboardSlow not loaded: Unable to find [MQTTDashboard] [[Slow]] config stanza")
            return
        # we have our config dict, now get my broker config dict
        broker_config_dict = get_mqtt_broker_dict(config_dict=config_dict,
                                                  service='Slow',
                                                  service_class_name='MqttDashboardSlow',
                                                  reqd_options=['server_url', 'topic'])
        # if our broker config dict is None we are unable to continue
        if broker_config_dict is None:
            return

        # initialize my superclass
        super(MqttDashboardSlow, self).__init__(engine, config_dict)

        # get our mqtt_debug level, if we don't have such a config option use
        # our parents
        mqtt_debug = slow_config_dict.get('mqtt_debug', self.mqtt_debug)
        # get an instance of class MQTTDashboardSlowThread and start the thread
        # running
        alt_m = weewx.units.convert(engine.stn_info.altitude_vt, 'meter').value
        self.thread = MqttDashboardSlowThread(queue=self.queue,
                                              config_dict=config_dict,
                                              slow_config_dict=slow_config_dict,
                                              server_config_dict=server_config_dict,
                                              lat=engine.stn_info.latitude_f,
                                              long=engine.stn_info.longitude_f,
                                              alt_m=alt_m,
                                              mqtt_debug=mqtt_debug)
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

    def __init__(self, queue, config_dict, slow_config_dict, server_config_dict,
                 lat, long, alt_m, mqtt_debug):

        # initialize my superclass
        threading.Thread.__init__(self)

        # set my thread name and run as a daemon
        self.setName('MqttDashboardSlowThread')
        self.setDaemon(True)
        # save our queue for receiving data
        self.queue = queue
        # save our config dict
        self.config_dict = config_dict

        # get the moon phase names to use, default to the WeeWX defaults
        self.moonphases = slow_config_dict.get('Almanac', {}).get('moon_phases',
                                                                  weeutil.Moon.moon_phases)

        # get a unit converter
        # first get the unit nickname ('US', 'METRIC' or 'METRICWX') we are to use,
        # default to 'METRIC'
        target_unit_nickname = slow_config_dict.get('target_unit', 'METRIC')
        # now translate that to a WeeWX unit system
        self.target_unit = weewx.units.unit_constants[target_unit_nickname.upper()]
        # now bind self.converter to the appropriate standard converter
        self.converter = weewx.units.StdUnitConverters[self.target_unit]

        # MQTT broker URL
        server_url = server_config_dict.get('server_url')
        # topic to publish to
        self.topic = server_config_dict.get('topic', 'weather/slow')
        # TLS options
        tls_opt = server_config_dict.get('tls', None)
        # quality of service
        qos = to_int(server_config_dict.get('tls', 0))
        # will MQTT broker retain messages
        retain = to_bool(server_config_dict.get('retain', True))
        # log successful publishing of data
        log_success = to_bool(server_config_dict.get('log_success', False))
        # save our mqtt_debug level
        self.mqtt_debug = mqtt_debug

        # do our config logging before the Publisher
        loginf("%s: Data will be published to '%s' under topic '%s'" % (self.name,
                                                                        obfuscate_password(server_url),
                                                                        self.topic))
        loginf("%s: qos is %d, retain is %r, log success is %s" % (self.name,
                                                                   qos,
                                                                   retain,
                                                                   log_success))

        # get an MQTTPublisher object to do the publishing for us
        self.publisher = MqttPublisher(server_url=server_url,
                                       tls=tls_opt,
                                       retain=retain,
                                       qos=qos,
                                       log_success=log_success,
                                       mqtt_debug=mqtt_debug)

        # save some station info
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt_m

        # initialise some properties that will pick up real values later
        self.stats = None
        self.db_binder = None
        self.almanac = None

    def run(self):
        """Collect records from the queue and manage their processing.

        Now that we are in a thread get a manager for our dbs (bindings). Once
        this is done we wait for something in the queue.
        """

        # Would normally do this in our class' __init__ but since we are
        # running in a thread we need to wait until the thread is actually
        # running before we can get any db related objects.

        # get a DBBinder object
        self.db_binder = weewx.manager.DBBinder(self.config_dict)

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
            if self.mqtt_debug >= 3:
                loginf("Received archive record: %s %s" % (timestamp_to_string(_package['dateTime']),
                                                         to_sorted_string(_package)))
            elif self.mqtt_debug == 2:
                loginf("Received archive record dateTime=%s" % timestamp_to_string(_package['dateTime']))
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
                                               trend=trend_dict)
            temperature_c = record.get('outTemp', 15.0)
            pressure_mbar = record.get('barometer', 1010.0)
            self.almanac = weewx.almanac.Almanac(record['dateTime'],
                                                 self.latitude,
                                                 self.longitude,
                                                 altitude=self.altitude_m,
                                                 temperature=temperature_c,
                                                 pressure=pressure_mbar,
                                                 moon_phases=self.moonphases)
            # get a data dict from which to construct our JSON data
            data = self.calculate(record)
            # connect to the mqtt broker
            self.publisher.connect()
            # publish the data
            self.publisher.publish(self.topic,
                                   json.dumps(data),
                                   data['dateTime'])
            # log the time taken to process this record
            if self.mqtt_debug >= 1:
                loginf("Record (%s) processed in %.5f seconds" % (record['dateTime'],
                                                                 (time.time() - t1)))
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
            record: data record received

        Returns:
            Dictionary containing the data to be posted.
        """

        # make sure our incoming record is indeed a dict
        record_d = dict(record)
        # obtain the incoming record timestamp
        ts = int(record_d['dateTime'])

        # initialise our result containing dict
        data = {}

        # now work through each of the fields to be included in the data dict
        # and populate each field

        # add dateTime and usUnits
        data['dateTime'] = ts
        data['usUnits'] = self.target_unit

        # add outTemp fields
        outtemp = dict()
        # yesterday
        outtemp['yest'] = dict()
        outtemp['yest']['min'] = to_float(self.stats.yesterday().outTemp.min.raw)
        outtemp['yest']['min_ts'] = self.stats.yesterday().outTemp.mintime.raw
        outtemp['yest']['max'] = to_float(self.stats.yesterday().outTemp.max.raw)
        outtemp['yest']['max_ts'] = self.stats.yesterday().outTemp.maxtime.raw
        # trend
        outtemp['trend'] = to_float(self.stats.trend(time_delta=3600).outTemp.raw)
        outtemp['24h_trend'] = to_float(self.stats.trend(time_delta=86400).outTemp.raw)
        # add outTemp fields to our data
        data['outTemp'] = outtemp

        # add barometer fields
        barometer = dict()
        # trend
        barometer['trend'] = to_float(self.stats.trend(time_delta=10800).barometer.raw)
        # add barometer fields to our data
        data['barometer'] = barometer

        # add dewpoint fields
        dewpoint = dict()
        # trend
        dewpoint['trend'] = to_float(self.stats.trend(time_delta=3600).dewpoint.raw)
        # add dewpoint fields to our data
        data['dewpoint'] = dewpoint

        # add outHumidity fields
        outhumidity = dict()
        # trend
        outhumidity['trend'] = to_float(self.stats.trend(time_delta=3600).outHumidity.raw)
        # add outHumidity fields to our data
        data['outHumidity'] = outhumidity

        # add wind fields
        wind = dict()
        # windGust
        wind['windGust'] = dict()
        # yesterday
        wind['windGust']['yest'] = dict()
        wind['windGust']['yest']['max'] = to_float(self.stats.yesterday().windGust.max.raw)
        wind['windGust']['yest']['max_ts'] = self.stats.yesterday().windGust.maxtime.raw
        # windrun
        wind['windrun'] = dict()
        if self.stats.yesterday().windrun.exists:
            _run = to_float(self.stats.yesterday().windrun.sum.raw)
        else:
            try:
                _run_km = self.stats.yesterday().windSpeed.avg.km_per_hour.raw * 24
                _run_vt = ValueTuple(_run_km, 'km', 'group_distance')
                _run = self.converter.convert(_run_vt).value
#                _run = to_float(self.formatter.toString(_run_c, addLabel=False))
            except TypeError:
                _run = None
        wind['windrun']['yest'] = _run
        # add wind fields to our data
        data['wind'] = wind

        # add rain fields
        rain = dict()
        # yesterday
        rain['yest'] = to_float(self.stats.yesterday().rain.sum.raw)
        # month to date
        rain['mtd'] = to_float(self.stats.month().rain.sum.raw)
        # year to date
        rain['ytd'] = to_float(self.stats.year().rain.sum.raw)
        # add rain fields to our data
        data['rain'] = rain

        # add radiation fields
        radiation = dict()
        # yesterday
        radiation['yest'] = dict()
        radiation['yest']['max'] = to_float(self.stats.yesterday().radiation.max.raw)
        radiation['yest']['max_ts'] = self.stats.yesterday().radiation.maxtime.raw
        # add radiation fields to our data
        data['radiation'] = radiation

        # add UV fields
        uv = dict()
        # yesterday
        uv['yest'] = dict()
        uv['yest']['max'] = to_float(self.stats.yesterday().UV.max.raw)
        uv['yest']['max_ts'] = self.stats.yesterday().UV.maxtime.raw
        # add UV fields to our data
        data['UV'] = uv

        # add sun fields
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

        # add moon fields
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

        # now return the completed data dict
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

        # save our mqtt_debug setting
        self.mqtt_debug = mqtt_debug
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
        Otherwise, if the on_connect callback does not respond with a successful
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
#                else:
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
        # set up the client callbacks
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
                # so the client thinks publication was a success, but what was
                # the actual result in res
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
#                      Supporting classes and functions
# ============================================================================


# ============================================================================
#                           class ObsBuffer
# ============================================================================

class ObsBuffer(object):
    """Base class to buffer an obs.

    An object that keeps a limited data history for one or more obs. The object
    can return various aggregates (eg max, sum or maxtime) based on the current
    data history. With appropriate methods the object can buffer scalar obs or
    vector obs.
    """

    def __init__(self, stats, units=None, history=False):
        self.units = units
        self.last = None
        self.lasttime = None
        if history:
            self.use_history = True
            self.history_full = False
            self.history = []
        else:
            self.use_history = False

    def add_value(self, val, ts, hilo=True):
        """Add a value to my hilo and history stats as required."""

        pass

    def day_reset(self):
        """Reset the vector obs buffer."""

        pass

    def trim_history(self, ts):
        """Trim any old data from the history list."""

        # calc ts of oldest sample we want to retain
        oldest_ts = ts - MAX_AGE
        # set history_full property
        self.history_full = min([a.ts for a in self.history if a.ts is not None]) <= oldest_ts
        # remove any values older than oldest_ts
        self.history = [s for s in self.history if s.ts > oldest_ts]


# ============================================================================
#                             class VectorBuffer
# ============================================================================

class VectorBuffer(ObsBuffer):
    """Class to buffer vector obs."""

    # default initialisation tuple, format is:
    # (min, mintime, max, maxtime, max_dir, sum, xsum, ysum, sumtime, count)
    default_init = (None, None, None, None, None, 0.0, 0.0, 0.0, 0.0, 0)

    def __init__(self, stats, units=None, history=False):
        # initialize my superclass
        super(VectorBuffer, self).__init__(stats, units=units, history=history)

        if stats:
            self.min = stats.min
            self.mintime = stats.mintime
            self.max = stats.max
            self.maxtime = stats.maxtime
            self.max_dir = stats.max_dir
            self.sum = stats.sum
            self.xsum = stats.xsum
            self.ysum = stats.ysum
            self.sumtime = stats.sumtime
            self.count = stats.count
            self.nineam_sum = 0.0
        else:
            (self.min, self.mintime,
             self.max, self.maxtime,
             self.max_dir, self.sum,
             self.xsum, self.ysum,
             self.sumtime, self.count) = VectorBuffer.default_init
            self.nineam_sum = 0.0

    def add_value(self, val, ts, hilo=True):
        """Add a value to my hilo and history stats as required.

        val: an object of type VectorTuple
        ts:  a unix epoch timestamp, may be None
        hilo: whether or not to track min/max values for this obs, boolean
        """

        if val.mag is not None:
            if hilo:
                if self.min is None or val.mag < self.min:
                    self.min = val.mag
                    self.mintime = ts
                if self.max is None or val.mag > self.max:
                    self.max = val.mag
                    self.max_dir = val.dir
                    self.maxtime = ts
            self.sum += val.mag
            self.nineam_sum += val.mag
            if self.lasttime:
                self.sumtime += ts - self.lasttime
            if val.dir is not None:
                self.xsum += val.mag * math.cos(math.radians(90.0 - val.dir))
                self.ysum += val.mag * math.sin(math.radians(90.0 - val.dir))
            if self.lasttime is None or ts >= self.lasttime:
                self.last = val
                self.lasttime = ts
            self.count += 1
            if self.use_history and val.dir is not None:
                self.history.append(ObsTuple(val, ts))
                self.trim_history(ts)

    def day_reset(self):
        """Reset the vector obs buffer to the defaults."""

        (self.min, self.mintime,
         self.max, self.max_dir,
         self.maxtime, self.sum,
         self.xsum, self.ysum,
         self.sumtime, self.count) = VectorBuffer.default_init

    def nineam_reset(self):
        """Reset the vector obs buffer nineam properties."""

        self.nineam_sum = 0.0

    @property
    def day_vec_avg(self):
        """The day average vector.

        Calculate the day average vector for a vector obs. Returns an object of
        type VectorTuple.
        """

        try:
            _magnitude = math.sqrt((self.xsum**2 + self.ysum**2) / self.sumtime**2)
        except ZeroDivisionError:
            return VectorTuple(0.0, 0.0)
        _direction = 90.0 - math.degrees(math.atan2(self.ysum, self.xsum))
        _direction = _direction if _direction >= 0.0 else _direction + 360.0
        return VectorTuple(_magnitude, _direction)

    @property
    def hist_vec_avg(self):
        """The history average vector.

        Calculate the vector average of the obs in the history buffer. The
        period over which the average is calculated is the history retention
        period (nominally 10 minutes).

        Returns an object of type VectorTuple. A result of (None, None) means
        there were no obs in the history buffer.
        """

        # TODO. Check the maths here, time ?
        result = VectorTuple(None, None)
        if self.use_history and len(self.history) > 0:
            xy = [(ob.value.mag * math.cos(math.radians(90.0 - ob.value.dir)),
                   ob.value.mag * math.sin(math.radians(90.0 - ob.value.dir))) for ob in self.history]
            xsum = sum(x for x, y in xy)
            ysum = sum(y for x, y in xy)
            oldest_ts = min(ob.ts for ob in self.history)
            _magnitude = math.sqrt((xsum**2 + ysum**2) / (time.time() - oldest_ts)**2)
            _direction = 90.0 - math.degrees(math.atan2(ysum, xsum))
            _direction = _direction if _direction >= 0.0 else _direction + 360.0
            result = VectorTuple(_magnitude, _direction)
        return result

    @property
    def hist_vec_dir(self):
        """The history vector average direction.

        Calculate the vector average direction of the obs in the history
        buffer. The period over which the average is calculated is the history
        retention period (nominally 10 minutes).

        Returns a number or None. A result of None means there were no obs in
        the history buffer.
        """

        # we can simply return the direction component of the hist_vec_avg
        # property
        return self.hist_vec_avg.dir


# ============================================================================
#                             class ScalarBuffer
# ============================================================================

class ScalarBuffer(ObsBuffer):
    """Class to buffer scalar obs."""

    # default initialisation tuple, format is:
    # (min, mintime, max, maxtime, sum, count)
    default_init = (None, None, None, None, 0.0, 0)

    def __init__(self, stats, units=None, history=False):
        # initialize my superclass
        super(ScalarBuffer, self).__init__(stats, units=units, history=history)

        # if we have a stats dict then use it to initialise my properties
        if stats:
            self.min = stats.min
            self.mintime = stats.mintime
            self.max = stats.max
            self.maxtime = stats.maxtime
            self.sum = stats.sum
            self.count = stats.count
            self.nineam_sum = 0.0
        else:
            # we have no stats dict so initialise with the defaults
            (self.min, self.mintime,
             self.max, self.maxtime,
             self.sum, self.count) = ScalarBuffer.default_init
            self.nineam_sum = 0.0

    def add_value(self, val, ts, hilo=True):
        """Add a value to my stats as required."""

        if val is not None:
            if hilo:
                if self.min is None or val < self.min:
                    self.min = val
                    self.mintime = ts
                if self.max is None or val > self.max:
                    self.max = val
                    self.maxtime = ts
            self.count += 1
            self.sum += val
            self.nineam_sum += val
            if self.lasttime is None or ts >= self.lasttime:
                self.last = val
                self.lasttime = ts
            if self.use_history:
                self.history.append(ObsTuple(val, ts))
                self.trim_history(ts)

    def day_reset(self):
        """Reset the scalar obs buffer."""

        (self.min, self.mintime,
         self.max, self.maxtime,
         self.sum, self.count) = ScalarBuffer.default_init

    def nineam_reset(self):
        """Reset the scalar obs buffer nineam properties."""

        self.nineam_sum = 0.0

    @property
    def hist_max(self, ts, age=MAX_AGE):
        """Return the max value in my history.

        Search the last 'age' seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp from which to start searching back
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is the maximum value and ts
            is the timestamp when it occurred.
        """

        born = ts - age
        # we are only interested in obs that are timestamped >= born
        snapshot = [a for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            _max = max(snapshot, key=itemgetter(1))
            return ObsTuple(_max[0], _max[1])
        else:
            return ObsTuple(None, None)

    @property
    def hist_avg(self, ts, age=MAX_AGE):
        """Return the average value in my history.

        Calculate the average of the obs in my history that are timestamped in
        the last 'age' seconds.

        Inputs:
            ts:  the timestamp from which to start searching back from,
                 nominally now
            age: the max age of the records being searched

        Returns:
            The average value, a result of None indicates there were no obs in
            the history period of concern.
        """

        born = ts - age
        snapshot = [a.value for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            return float(sum(snapshot)/len(snapshot))
        else:
            return None


# ============================================================================
#                               class Buffer
# ============================================================================

class Buffer(dict):
    """Class to buffer various loop packet obs.

    Archive based stats are an efficient means of getting stats for today.
    However, their use would mean that any daily stats (eg today's max outTemp)
    that occur after the most recent archive record but before the next
    archive record is written to archive will not be captured. For this reason
    selected loop data is buffered to ensure that such stats are correctly
    reflected.

    The Buffer object has many similarities with the WeeWX accumulator, but the
    Buffer object retains historical obs data for the period concerned.
    """

    def __init__(self, manifest, day_stats, additional_day_stats=None):
        """Initialise an instance of our class."""

        self.manifest = manifest
        # seed our buffer objects from day_stats
        for obs in [f for f in day_stats if f in self.manifest]:
            seed_func = seed_functions.get(obs, Buffer.seed_scalar)
            seed_func(self, day_stats, obs, history=obs in HIST_MANIFEST)
        # seed our buffer objects from additional_day_stats
        if additional_day_stats is not None:
            for obs in [f for f in additional_day_stats if f in self.manifest]:
                if obs not in self:
                    seed_func = seed_functions.get(obs, Buffer.seed_scalar)
                    seed_func(self, additional_day_stats, obs,
                              history=obs in HIST_MANIFEST)
        # timestamp of the last packet containing windSpeed, used for windrun
        # calcs
        self.last_windSpeed_ts = None

    def seed_scalar(self, stats, obs_type, history):
        """Seed a scalar buffer."""

        self[obs_type] = init_dict.get(obs_type, ScalarBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=history)

    def seed_vector(self, stats, obs_type, history):
        """Seed a vector buffer."""

        self[obs_type] = init_dict.get(obs_type, VectorBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=history)

    def add_packet(self, packet):
        """Add a packet to the buffer."""

        # the packet must have a timestamp, if not discard it
        if packet['dateTime'] is not None:
            # now iterate over all obs in the packet that are in our manifest
            for obs in [f for f in packet if f in self.manifest]:
                # get the add function
                add_func = add_functions.get(obs, Buffer.add_value)
                # call the add function
                add_func(self, packet, obs)

    def add_value(self, packet, obs):
        """Add a value to the buffer."""

        # if we haven't seen this obs before add it to our buffer
        if obs not in self:
            self[obs] = init_dict.get(obs, ScalarBuffer)(stats=None,
                                                         units=packet['usUnits'],
                                                         history=obs in HIST_MANIFEST)
        if self[obs].units == packet['usUnits']:
            _value = packet[obs]
        else:
            (unit, group) = weewx.units.getStandardUnitType(packet['usUnits'],
                                                            obs)
            _vt = ValueTuple(packet[obs], unit, group)
            _value = weewx.units.convertStd(_vt, self[obs].units).value
        self[obs].add_value(_value, packet['dateTime'])

    def add_wind_value(self, packet, obs):
        """Add a wind value to the buffer."""

        # first add it as a scalar
        self.add_value(packet, obs)

        # if there is no windrun in the packet and if obs is windSpeed then we
        # can use windSpeed to update windrun
        if 'windrun' not in packet and obs == 'windSpeed':
            # has windrun been seen before, if not add it to the Buffer
            if 'windrun' not in self:
                self['windrun'] = init_dict.get(obs, ScalarBuffer)(stats=None,
                                                                   units=packet['usUnits'],
                                                                   history=obs in HIST_MANIFEST)
            # to calculate windrun we need a speed over a period of time, are
            # we able to calculate the length of the time period?
            if self.last_windSpeed_ts is not None:
                windrun = self.calc_windrun(packet)
                self['windrun'].add_value(windrun, packet['dateTime'])
            self.last_windSpeed_ts = packet['dateTime']

        # now add it as the special vector 'wind'
        if obs == 'windSpeed':
            if 'wind' not in self:
                self['wind'] = VectorBuffer(stats=None, units=packet['usUnits'])
            if self['wind'].units == packet['usUnits']:
                _value = packet['windSpeed']
            else:
                (unit, group) = weewx.units.getStandardUnitType(packet['usUnits'],
                                                                'windSpeed')
                _vt = ValueTuple(packet['windSpeed'], unit, group)
                _value = weewx.units.convertStd(_vt, self['wind'].units).value
            self['wind'].add_value(VectorTuple(_value, packet.get('windDir')),
                                   packet['dateTime'])

    def start_of_day_reset(self):
        """Reset our buffer stats at the end of an archive day.

        Call the day_reset() method for each obs in our manifest that we have
        seen.
        """

        for obs in self.manifest:
            if obs in self:
                self[obs].day_reset()

    def nineam_reset(self):
        """Reset our nineam buffer stats.

        Call the nineam_reset() method for each obs in our manifest that we
        have seen.
        """

        for obs in self.manifest:
            if obs in self:
                self[obs].nineam_reset()

    def calc_windrun(self, packet):
        """Calculate windrun given windSpeed."""

        val = None
        if packet['usUnits'] == weewx.US:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts) / 3600.0
            unit = 'mile'
        elif packet['usUnits'] == weewx.METRIC:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts) / 3600.0
            unit = 'km'
        elif packet['usUnits'] == weewx.METRICWX:
            val = packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts)
            unit = 'meter'
        if self['windrun'].units == packet['usUnits']:
            return val
        else:
            _vt = ValueTuple(val, unit, 'group_distance')
            return weewx.units.convertStd(_vt, self['windrun'].units).value


# ============================================================================
#                   Class Buffer configuration dictionaries
# ============================================================================

# in most cases we will use the default, here we can define those that need
# special attention
init_dict = weewx.units.ListOfDicts({'wind': VectorBuffer})
add_functions = weewx.units.ListOfDicts({'windSpeed': Buffer.add_wind_value})
seed_functions = weewx.units.ListOfDicts({'wind': Buffer.seed_vector})


# ============================================================================
#                              class ObsTuple
# ============================================================================

class ObsTuple(tuple):
    """Class supporting timestamped observations.

    An observation during some period can be represented by the value of the
    observation and the time at which it was observed. This can be represented
    in a two-way tuple called an obs tuple. An obs tuple is useful because its
    contents can be accessed using named attributes.

    Item   attribute   Meaning
     0     value       The observed value eg 19.5, it can also be an object of
                       type VectorTuple to represent a vector type obs
     1     ts          The epoch timestamp that the value was observed
                       eg 1488245400

    It is valid to have an observed value of None.

    It is also valid to have a ts of None (meaning there is no information
    about the time the obs was observed).
    """

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    @property
    def value(self):
        return self[0]

    @property
    def ts(self):
        return self[1]


# ============================================================================
#                              class VectorTuple
# ============================================================================

class VectorTuple(tuple):
    """Class representing a vector observation.

    A vector value can be represented as a magnitude and direction. This can be
    represented in a 2 way tuple called a vector tuple. A vector tuple is
    useful because its contents can be accessed using named attributes.

    Item   attribute   Meaning
     0     mag         The magnitude of the vector
     1     dir         The direction of the vector in degrees

    mag and dir may be None.
    """

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    @property
    def mag(self):
        return self[0]

    @property
    def dir(self):
        return self[1]


# ============================================================================
#                             Utility functions
# ============================================================================

def get_mqtt_broker_dict(config_dict, service, service_class_name, reqd_options=None):
    """Obtain MQTT broker options for a given MQTTDashboard service.

    Obtains the MQTT broker options, with defaults, from the relevant MQTT
    service stanza under [MQTTDashboard]. If the MQTT service is not enabled,
    or if one or more required options is not specified, then return None.
    """

    try:
        broker_dict = weeutil.config.accumulateLeaves(config_dict['MQTTDashboard'][service]['MQTT'],
                                                      max_level=2)
    except KeyError:
        # we couldn't find the MQTT stanza for our broker
        loginf("Service %s not loaded: No MQTT broker info." % service_class_name)
        return None

    # if broker_dict has the key 'enable' and it is False, then the service is
    # not enabled
    try:
        if not to_bool(broker_dict['enable']):
            loginf("Service %s not loaded: Service is not enabled." % service_class_name)
            return None
    except KeyError:
        pass

    # At this point, either the key 'enable' does not exist, or it is set to
    # True. Check to see whether all the required options exist, and none of
    # them have been set to 'replace_me'.
    if reqd_options is not None:
        try:
            for option in reqd_options:
                if broker_dict[option] == 'replace_me':
                    raise KeyError(option)
        except KeyError as e:
            logdbg("Service %s not loaded: Missing option %s" % (service_class_name, e))
            return None

    # If the broker dictionary does not have a log_success or log_failure, get
    # them from the root dictionary
    broker_dict.setdefault('log_success', to_bool(config_dict.get('log_success', True)))
    broker_dict.setdefault('log_failure', to_bool(config_dict.get('log_failure', True)))

    # if it exists get rid of the no longer needed key 'enable'
    broker_dict.pop('enable', None)

    return broker_dict


def obfuscate_password(url, o_str='xxx'):
    """Obfuscate the 'password' portion of a URL.

    Splits the URL into it's six component parts and then obfuscates the
    password subcomponent of the netloc component. The URL is re-assembled
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
        # there are no query parameters so return the original url
        return url
