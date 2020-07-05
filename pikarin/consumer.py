import random

import requests
import pika

class RabbitMQConsumer(object):

    ## Override this properties 
    config = None

    def __init__(self, config=None):
        self.debug = False
        self.node_discovery_method = ''
        self.nodes = []
        # set config
        if config != None:
            self.verify_config(config)
        else:
            self.verify_config(self.config)
    
    def verify_config(self, config):
        # check is config exist?
        if config == None:
            raise Exception('config is not defined')

        # check debug level
        if 'debug' in config:
            if config['debug'] == True:
                self.debug = True
            else:
                self.debug = False

        ## check rabbitmq node_discovery_method
        if 'node_discovery_method' in config:
            self.node_discovery_method = config['node_discovery_method']
            if self.node_discovery_method == 'priority':
                pass
            elif self.node_discovery_method == 'random':
                pass
            else:
                raise Exception('Unknown method {0} on \'node_discovery_method\' in config'.format(self.node_discovery_method))
        else:
            raise Exception('\'node_discovery_method\' is required on config')

        ## check rabbitmq node
        if 'nodes' in config:
            if type(config['nodes']) != list:
                raise Exception('\'nodes\' should list of dictionary')
            
            if len(config['nodes']) > 1:
                self.nodes = config['nodes']
            else:
                raise Exception('length \'nodes\' must be greater than 0')
        else:
            raise Exception('\'nodes\' is required on config')

    def set_node(self):
        # decide nodes to connect
        # based on node_discovery_method
        self.connection = None
        self.channel = None
        if self.node_discovery_method == 'priority':
            for item in self.nodes:
                if self.connection == None and self.channel == None:
                    try:
                        self.credentials = pika.PlainCredentials(item['user'], item['password'])
                        self.connection = pika.BlockingConnection(
                            pika.ConnectionParameters(host=item['host'], port=item['port'], credentials=self.credentials)
                        )
                        self.channel = self.connection.channel()
                    except:
                        self.connection = None
                        self.channel = None

        elif self.node_discovery_method == 'random':
            pass
        
        ## if there are no nodes active
        if self.connection == None and self.channel == None:
            raise Exception('All Nodes seems down or unreachable')
    
    ## Override this method
    def on_message(self, ch, method, properties, body):
        print('Please override on_message method on RabbitMQConsumer')
        print(ch)
        print(method)
        print(properties)
        print(body)
        return body

    def callback(self, ch, method, properties, body):
        try:
            response = self.on_message(ch, method, properties, body)
        except Exception as e:
            if self.debug:
                print(e)
            
            # if error change response
            # using error message
            response = {
                'error': str(e)
            }
        finally:
            # if need rpc
            if properties.reply_to != None and properties.correlation_id != None:
                ch.basic_publish(exchange='', 
                                routing_key=properties.reply_to,
                                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                                body=str(response))

    def consume(self, queue=''):
        active = True
        while active:
            print('retry to connect')
            self.set_node() # choose nodes to connect

            ## declare queue 
            # (if queue is an empty string, queue will be generate as random)
            try:
                result = self.channel.queue_declare(queue=queue, 
                                                    exclusive=False, 
                                                    durable=True)
                self.consume_queue = result.method.queue
            except:
                # if nodes down the queue on that nodes
                # cannot be accessible. The consumer should
                # delete queue and recreate that queue
                self.channel.queue_delete(queue)
                result = self.channel.queue_declare(queue=queue, 
                                                    exclusive=False, 
                                                    durable=True)
                self.consume_queue = result.method.queue
            
            ## start consuming
            try:
                self.channel.basic_consume(
                    queue=self.consume_queue,
                    on_message_callback=self.callback,
                    auto_ack=True
                )
                print('start consuming queue {0}'.format(self.consume_queue))
                self.channel.start_consuming()
            except KeyboardInterrupt:
                print('keyboard interrupt')
                active = False
                self.connection.close()
