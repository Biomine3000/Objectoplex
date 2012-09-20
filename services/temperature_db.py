# -*- coding: utf-8 -*-
import json
import codecs

import ConfigParser

from os.path import expanduser
from datetime import datetime, timedelta
from optparse import OptionParser

import psycopg2, sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import sessionmaker

from system import BusinessObject
from service import Service


def postgres_url(config):
    return u"postgresql://{0}:{1}@localhost:5432/{2}".format(config.get('database', 'username'),
                                                             config.get('database', 'password'),
                                                             config.get('database', 'database'))

def read_config(path):
    config = ConfigParser.ConfigParser()
    with codecs.open(path, 'r') as f:
        config.readfp(f)
    return config


class NotImplemented(Exception): pass

class TemperatureReading(Base):
    __tablename__ = 'temperature_reading'

    id = Column(Integer, primary_key=True)
    sensor = Column(String)
    value = Column(Float)
    created = Column(DateTime)

    def __init__(self, sensor, value):
        self.sensor = sensor
        self.value = value
        self.created=datetime.utcnow()

    def to_dict(self):
        return { u"sensor": self.sensor,
                 u"value": self.value,
                 u"created": self.created.isoformat() }


class TemperatureDB(Service):
    __service__ = 'temperature_db'

    def __init__(self, *args, **kwargs):
        super(TemperatureDB, self).__init__(*args, **kwargs)
        self.logger.info("Accepted parameters (key=value): config-file (ini-format with section 'database' containing 'username', 'password', 'database', 'echo')")
        self.config_file = expanduser(self.args['config-file'])
        self.config = read_config(self.config_file)
        self.init_db_session()

    def init_db_session(self):
        self.engine = create_engine(postgres_url(self.config),
                                    echo=self.config.getboolean('database', 'echo'))
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def ensure_connection_up(self):
        def simple_query():
            result = self.session.execute(u"SELECT 1;")
            res = result.fetchone()

        try:
            simple_query()
        except sqlalchemy.exc.OperationalError, oe:
            self.logger.warning("Test query to the server failed, restarting database connection... (%s)" % str(oe.orig).strip())
            self.init_db_session()
            simple_query()
            self.logger.warning("Database connection successfully re-initialized!")
        except Exception, e:
            import traceback
            traceback.print_exc()
            raise e

    def handle(self, obj):
        self.logger.debug(u"Request {0}".format(obj.metadata))

        reply = None
        error = None

        try:
            self.ensure_connection_up()

            request = obj.metadata['request']
            if request == 'insert':
                reply = self.insert(obj)
            elif request == 'last':
                reply = self.last(obj)
            elif request == 'sensors':
                reply = self.sensors(obj)
            else:
                raise NotImplemented("Request type '%s' is not implemented!" % request)
        except Exception, e:
            import traceback
            traceback.print_exc()
            error_string = "Encountered %s.%s: %s" % (e.__class__.__module__,
                                                      e.__class__.__name__, str(e).strip())
            self.logger.warning(error_string)
            error = error_string

        metadata = { 'event': 'services/reply',
                     'in-reply-to': obj.id,
                     'type': 'text/json' }

        if 'route' in obj.metadata:
            metadata['to'] = obj.metadata['route'][0]

        if error:
            reply = { 'error': str(error) }

        payload = bytearray(json.dumps(reply, ensure_ascii=False), encoding='utf-8')
        metadata['size'] = len(payload)

        return BusinessObject(metadata, payload)

    def insert(self, obj):
        payload = json.loads(obj.payload.decode('utf-8'))

        readings = []

        for item in payload:
            readings.append(TemperatureReading(item['sensor'], item['value']))

        for reading in readings:
            self.session.add(reading)

        self.session.commit()
        return {u'status': 'Success!'}

    def last(self, obj):
        payload = json.loads(obj.payload.decode('utf-8'))
        q = self.session.query(TemperatureReading).filter(TemperatureReading.sensor==payload['sensor'])
        q = q.order_by(TemperatureReading.created.desc())
        return q.first().to_dict()

    def sensors(self, obj):
        sensors = self.session.query(TemperatureReading).distinct(TemperatureReading.sensor)
        return [r.sensor for r in sensors.all()]


service = TemperatureDB


def main():
    parser = OptionParser()
    parser.add_option("--config-file", dest="config_file", metavar='FILE')
    parser.add_option("--create-tables", dest="create_tables", default=False, action="store_true")
    opts, args = parser.parse_args()

    if not opts.config_file:
        parser.error("Config file required!")

    if opts.create_tables:
        config = read_config(opts.config_file)

        engine = create_engine(postgres_url(config), echo=True)
        # engine = create_engine('sqlite:///:memory:', echo=True)
        Base.metadata.create_all(engine)


if __name__ == '__main__':
    main()
