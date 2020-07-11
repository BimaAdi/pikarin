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

        # check rabbitmq node_discovery_method
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

        # check rabbitmq node
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
        """
        decide nodes to connect
        based on node_discovery_method
        """
        self.connection = None
        self.channel = None
        connected_node = None

        if self.node_discovery_method == 'priority':
            # priority => try toconnect to every node from top to bottom
            # of list of config nodes
            for item in self.nodes:
                if self.connection == None and self.channel == None:
                    try:
                        self.credentials = pika.PlainCredentials(item['user'], item['password'])
                        self.connection = pika.BlockingConnection(
                            pika.ConnectionParameters(host=item['host'], port=item['port'], credentials=self.credentials)
                        )
                        self.channel = self.connection.channel()
                        # node connected successfully
                        connected_node = item
                    except:
                        self.connection = None
                        self.channel = None

        elif self.node_discovery_method == 'random':
            # random => connect randomly to every node
            # node that failed to connect will not be connected twice
            try_nodes = self.nodes
            index_try_nodes = [*range(len(try_nodes))] # need index to remove item from list of dictionary
            while self.connection == None and self.channel == None and len(try_nodes) > 0:
                index_try_node = random.choice(index_try_nodes)
                try_node = try_nodes[index_try_node]
                try:
                    self.credentials = pika.PlainCredentials(try_node['user'], try_node['password'])
                    self.connection = pika.BlockingConnection(
                        pika.ConnectionParameters(host=try_node['host'], port=try_node['port'], credentials=self.credentials)
                    )
                    self.channel = self.connection.channel()
                    # node connected successfully
                    connected_node = try_node
                except:
                    self.connection = None
                    self.channel = None
                    # remove node with failed connection from list
                    try_nodes.pop(index_try_node)
                    index_try_nodes = [*range(len(try_nodes))]
        
        # if there are no nodes active
        if self.connection == None and self.channel == None:
            raise Exception('All Nodes seems down or unreachable')
        else:
            return connected_node
    
    ## Override this method
    def on_message(self, ch, method, properties, body):
        """
        Method that will be called when consumer get message
        """
        print('Please override on_message method on RabbitMQConsumer')
        print('ch:')
        print(ch)
        print('method:')
        print(method)
        print('properties:')
        print(properties)
        print('body:')
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
            self.connected_node = self.set_node() ## choose nodes to connect
            print(self.connected_node)

            # declare queue 
            # (if queue is an empty string, queue will be generate as random)
            try:
                ## exclusive = True, so any consumer can connect
                ## durable = False, so queue can be deleted
                ## auto_delete = True, delete queue on consumer or node down, Because
                ##              consumer cannot switch to another node if previous
                ##              queue still exist
                result = self.channel.queue_declare(queue=queue, 
                                                    exclusive=False, 
                                                    durable=False,
                                                    auto_delete=True)
                self.consume_queue = result.method.queue
            except:
                ## if a node down the queue on that node
                ## cannot be accessible. The consumer should
                ## delete queue and recreate that queue
                self.channel.queue_delete(queue)
                result = self.channel.queue_declare(queue=queue, 
                                                    exclusive=False, 
                                                    durable=True,
                                                    auto_delete=True)
                self.consume_queue = result.method.queue
            
            # start consuming
            try:
                self.consumer_tag = self.channel.basic_consume(
                    queue=self.consume_queue,
                    on_message_callback=self.callback,
                    auto_ack=True
                )

                ## print consumer information
                print('start consuming')
                print('consumer information:')
                print('consumer_tag: {0}'.format(self.consumer_tag))
                print('consumed queue: {0}'.format(self.consume_queue))
                print('connected node: {0}:{1}'.format(self.connected_node['host'], self.connected_node['port']))
                self.channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                # On node down
                # retry to connect
                print('node {0}:{1} seems down, try to connect to another node'.format(self.connected_node['host'], self.connected_node['port']))
                continue
            except KeyboardInterrupt:
                print('keyboard interrupt')
                active = False
                self.connection.close()
