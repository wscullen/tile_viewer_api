#heck connection to the RabbitMQ server

# Source: https://blog.sleeplessbeastie.eu/2017/07/10/how-to-check-connection-to-the-rabbitmq-message-broker/ 



# import parser for command-line options

import argparse

# import a pure-Python implementation of the AMQP 0-9-1 

import pika



# define and parse command-line options

parser = argparse.ArgumentParser(description='Check connection to RabbitMQ server')

parser.add_argument('--server', required=True, help='Define RabbitMQ server')

parser.add_argument('--virtual_host', default='/', help='Define virtual host')

parser.add_argument('--ssl', action='store_true', help='Enable SSL (default: %(default)s)')

parser.add_argument('--port', type=int, default=5672, help='Define port (default: %(default)s)')

parser.add_argument('--username', default='guest', help='Define username (default: %(default)s)')

parser.add_argument('--password', default='guest', help='Define password (default: %(default)s)')

args = vars(parser.parse_args())



# set amqp credentials

credentials = pika.PlainCredentials(args['username'], args['password'])

# set amqp connection parameters

parameters = pika.ConnectionParameters(host=args['server'], port=args['port'], virtual_host=args['virtual_host'], credentials=credentials, ssl=args['ssl'])



# try to establish connection and check its status

try:

  connection = pika.BlockingConnection(parameters)

  if connection.is_open:

    print('OK')

    connection.close()

    exit(0)

except Exception as error:

  print('Error:', error.__class__.__name__)

  exit(1)
