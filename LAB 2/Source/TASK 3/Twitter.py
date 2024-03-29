from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time

consumer_key = 'RiAljDExhBmC58Iqh0fyv3Kia'
consumer_secret = '4Bo2UzGErfBGJARHVBA0zkrA3pLCxgj916MwDJzwbdp3PwVUFn'
access_token = '391206417-Ws9xmwz1zfJBGD6dlj7RNej8EroI3e26kRpwov5d'
access_secret = 'LDbm2pGU7IMQ0QiACTmnokPLePxfSW01eR5K8R64YNTQy'


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['films'])

if __name__ == "__main__":
    s = socket.socket()
    host = "localhost"
    port = 1235
    s.bind((host, port))
    print("Listening on port: %s" % str(port))
    s.listen(5)
    c, addr = s.accept()
    print("Received request from: " + str(addr))
    time.sleep(5)
    sendData(c)