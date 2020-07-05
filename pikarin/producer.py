import random
import uuid

import requests
import pika

class RabbitMQProducer(object):

    rpc = False
    publish_queue = ''
    message = 'Test'
    config = None

    def __init__(self, config=None, rpc=None):
        self.debug = False
        self.node_discovery_method = ''
        self.nodes = []
        if config != None:
            self.verify_config(config)
        else:
            self.verify_config(self.config)

        self.set_node()
        # set rpc if defined
        if rpc != None:
            self.rpc = rpc
        if self.rpc == None:
            raise Exception('rpc is not defined')

        if self.rpc == True:
            ## Prepare callback queue and consumer
            self.response = None
            self.corr_id = str(uuid.uuid4())
            result = self.channel.queue_declare(queue='', exclusive=False)
            self.callback_queue = result.method.queue
            
            self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        
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
    
    ## decide which nodes to connect
    def set_node(self):
        ## based on node_discovery_method
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

    ## on_response for callback
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    ## Override this method
    def set_message(self, message):
        return message

    ## Override this method
    def set_publish_queue(self, message):
        return self.publish_queue

    def runner_set_message_function(self, message):
        return self.set_message(message)

    def runner_set_publish_queue_function(self, message):
        return self.set_publish_queue(message)

    def publish(self, message):
        ## set message if set_message function
        ## can be override
        self.message = self.runner_set_message_function(self.message)

        ## decide which queue to publish
        ## can be override
        self.publish_queue = self.runner_set_publish_queue_function(self.message)

        ## if using rpc wait for response
        if self.rpc == True:
            ## publish the message and properties
            self.channel.basic_publish(exchange='', 
                                        routing_key=self.publish_queue, 
                                        properties=pika.BasicProperties(
                                            reply_to=self.callback_queue,
                                            correlation_id=self.corr_id,
                                        ),
                                        body=str(message))
            try:
                while self.response is None:
                    self.connection.process_data_events()

                self.channel.queue_delete(self.callback_queue)
                self.connection.close()
                return self.response
            except KeyboardInterrupt:
                self.channel.queue_delete(self.callback_queue)
                self.connection.close()
        else:
            ## publish the message withput properties
            self.channel.basic_publish(exchange='', 
                                        routing_key=self.publish_queue, 
                                        body=str(message))
            