#!/usr/bin/env python3

# An application to sniff all of the messages published on the HPE OneView
# System Change Message Bus (SCMB).  See the README.md in the parent directory
# for details.

import hpOneView as hpov
import os
import sys
import json
import argparse
import tempfile
import ssl
import pika
from pika.credentials import ExternalCredentials

def parse_args():
    parser = argparse.ArgumentParser(description='Listen for messages on the OneView SCMB',
                                     epilog='To terminate listening press control-c')
    parser.add_argument('-s', '--server', type=str, required=True, help='hostname or address of the OneView server')
    parser.add_argument('-u', '--user', type=str, required=True, help='username to use when connecting to the OneView server')
    parser.add_argument('-p', '--password', type=str, required=True, help='password to use when connecting to the OneView server')
    parser.add_argument('-t', required=False, action='store_true', help='prefix each message with the timestamp')
    parser.add_argument('-j', required=False, action='store_true', help='print the JSON body of the message rather than the routing key')
    parser.add_argument('-r', required=False, action='store_false', help='suppress printing the routing key, i.e. the default is to print the routing key')
    parser.add_argument('routing_key', type=str, help='AMQP routing key to listen on')

    return parser.parse_args()

def main(args):
    try:
        ov = OVServer(args)
    except Exception as e:
        print('ERROR: Failed to login to the HPE OneView server', file=sys.stderr)
        sys.exit(1)

    ov.get_certificates()

    try:
        ov.scmb_connect(args.routing_key)
    except KeyboardInterrupt as e:
        # Catching control-c to exit
        pass
    except Exception as e:
        print(e)
    finally:
        ov.cleanup()
        sys.exit(0)

class OVServer(object):
    def __init__(self, args):
        self.server = args.server
        self.username = args.user
        self.password = args.password

        self.print_timestamp = args.t
        self.print_json = args.j
        self.print_routing_key = args.r

        self.con = hpov.connection(self.server)
        self.con.login({'userName': self.username, 'password': self.password})

    def cleanup(self):
        os.remove(os.path.join(self.tempdir, 'caroot.pem'))
        os.remove(os.path.join(self.tempdir, 'cert.pem'))
        os.remove(os.path.join(self.tempdir, 'key.pem'))
        os.rmdir(self.tempdir)

    def get_certificates(self):
        try:
            sec = hpov.security(self.con)
            act = hpov.activity(self.con)
            body = sec.gen_rabbitmq_internal_signed_ca()
            count = 0
            while act.is_task_running(task):
                time.sleep(1)
                count += 1
                if count > 60:
                    print('ERROR: Timed out generating certificates.', file=sys.stderr)
                    sys.exit(1)

            task = con.get(task['uri'])
            if task['taskState'] in TaskErrorStates and task['taskState'] != 'Warning':
                message = task['taskErrors'][0]['message']
                if message is not None:
                    print('ERROR: Got an error during certificate generation: ' + message, file=sys.stderr)
                    sys.exit(1)
        except hpov.HPOneViewException as e:
            # Ignoring as it just means that the cert already exists for download
            pass

        # Get the CA Bundle, Cert, and Keys
        self.tempdir = tempfile.mkdtemp()
        try:
            sec = hpov.security(self.con)
            cert = sec.get_cert_ca()
            ca = open(os.path.join(self.tempdir, 'caroot.pem'), 'w+')
            ca.write(cert)
            ca.close()
            cert = sec.get_rabbitmq_kp()
            ca = open(os.path.join(self.tempdir, 'cert.pem'), 'w+')
            ca.write(cert['base64SSLCertData'])
            ca.close()
            ca = open(os.path.join(self.tempdir, 'key.pem'), 'w+')
            ca.write(cert['base64SSLKeyData'])
            ca.close()
        except Exception as e:
            self.cleanup()
            self.con.logout()
            print('ERROR: Failed to save certificates: ' + e, file=sys.stderr)
            sys.exit(1)
        finally:
            self.con.logout()

    def scmb_connect(self, routing_key):
        ssl_options = ({'ca_certs': os.path.join(self.tempdir, 'caroot.pem'),
                        'certfile': os.path.join(self.tempdir, 'cert.pem'),
                        'keyfile': os.path.join(self.tempdir, 'key.pem'),
                        'cert_reqs': ssl.CERT_REQUIRED,
                        'server_side': False})

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                self.server, 5671, credentials=ExternalCredentials(),
                ssl=True, ssl_options=ssl_options))

        channel = connection.channel()
        result = channel.queue_declare()
        queue_name = result.method.queue
        channel.queue_bind(exchange='scmb', queue=queue_name, routing_key=routing_key)
        channel.basic_consume(self.callback, queue=queue_name, no_ack=True)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        msg = json.loads(body.decode('utf-8'))
        if self.print_timestamp:
            print(msg['timestamp'] + ': ', end='', flush=True)
        if self.print_routing_key:
            print(method.routing_key)
        if self.print_json:
            print(msg)

if __name__ == '__main__':
    main(parse_args())
    sys.exit(0)
