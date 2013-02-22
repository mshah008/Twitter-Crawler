import tweetstream, twitter
import igraph
import multiprocessing
import Queue
import time
import cPickle
#from pybloomfilter import BloomFilter
import MySQLdb
import elixir
import simplejson as json
import logging
import sqlalchemy


"""
A multi-process crawler for twitter. It creates 50 processes that are used to
query twitter. Postgresql database is used to store the data. Elixir
library is used for interacting with the database. The main process is the
manager, which puts the new unseen twitter users in the request queue, and reads
the results from the result queue and writes them in the database. The 50 worker
processes have the simple duty of reading the new requests from the request
queue, fetching them from twitter, and putting the result in the result queue.

"""

MAX_THREAD_NUM=100
PROCESS_NUM=50
MAX_NODE_NUM = 1000000
cap_wait = 5 * 60#seconds
max_wait = 60*60#seconds
outfile = "links"


class myLogging():
    def __init__(self, logq):
        self.logq = logq

    def log(self, msg):
        self.logq.put(msg)
        
            


def initiate_elixir():
    #elixir.metadata.bind = "mysql://root:mysqlp@ss@localhost/twitter_crawl"
    #elixir.metadata.bind = "sqlite:///twitter_crawl.sqlite"
    elixir.metadata.bind = "postgresql://ali:postgresp@ss@localhost/twitter_crawl"
    elixir.setup_all()
    elixir.create_all()

def renew_elixir_session():
    elixir.metadata.bind = "postgresql://ali:postgresp@ss@localhost/twitter_crawl"
    elixir.session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker())
#def connect_to_db():
#    conn = MySQLdb.connect (host = "localhost", user="root", passwd="mysqlp@ss", db="netsa")
#
#    return conn

def terminate_children(qreq):
    for i in range(PROCESS_NUM):
        qreq.put("TERMINATE")

def getidnum(uname):
    pass

def logger_process(qlogs):
    loglevel = logging.INFO
    logging.basicConfig(filename= "twitter_crawler.log", level=loglevel)
    logger = logging.getLogger('twitter_crawler')

    while True:
        l = qlogs.get()
        logger.info(l)
        if l == "TERMINATE":
            break

def file_writer_process(fq):
    
    lf = open("links.txt", "wt")
    af = open("attr.txt", "wt")
    while True:
        item = fq.get()
        if item == "TERMINATE":
            break
        else:
            if item[0] == "links":
                lf.write(item[1])
            else:
                af.write(item[1])

    lf.close()
    af.close()



def multiprocess_mine(seeds):
    processes = []
    qreq = multiprocessing.JoinableQueue()
    qres = multiprocessing.JoinableQueue(maxsize=1000)
    qlogs = multiprocessing.JoinableQueue()
    mylogger = myLogging(qlogs)
    fq = multiprocessing.JoinableQueue()
    dblock = multiprocessing.Lock()
    #users_requested = BloomFilter(100000000, 0.01, "requested.bloom")
    users_requested_num = 0
    #users_done = BloomFilter(100000000, 0.01, "done.bloom")
    users_done_num = 0
    processes.append(multiprocessing.Process(target=file_writer_process, args=(fq,)))
    processes.append(multiprocessing.Process(target=logger_process, args=(qlogs,)))
    for i in range(PROCESS_NUM):
        processes.append(multiprocessing.Process(target=worker, args=(qreq,qres,fq,qlogs,dblock, )))


    try:
        for p in processes:
            p.start()
        
        from model import *
        initiate_elixir()
        for uname in seeds:
            #idnum = getidnum(uname)
            print UserAttr.query.filter_by(uname=uname).count()
            if UserAttr.query.filter_by(uname=uname).count() == 0:
                mylogger.log("putting " +  uname)
                qreq.put(uname)
                #users_requested.add(uname)
                user_requested = UserAttr(uname)
                user_requested.requested = True
        
                users_requested_num += 1
        if qreq.qsize() < PROCESS_NUM:
            new_reqset = UserAttr.query.filter_by(requested=False).slice(1, 10 * PROCESS_NUM)
            for new_req in new_reqset:
                new_req.requested = True
                mylogger.log("putting " +  new_req.uname)
                qreq.put(new_req.uname)
                users_requested_num += 1

        elixir.session.commit()
        while users_done_num < MAX_NODE_NUM:
            res = qres.get()
            #uname = res.keys()[0]
            #item = res.values()[0]
            #user = UserAttr.query.filter_by(uname=uname).one()
            
            #users_done.add(res.keys()[0])
            users_done_num += 1
            #friends = [x.screen_name for x in item[0]]
            #followers = [x.screen_name for x in item[1]]
            #user.SetFriends(friends)
            #user.SetFollowers(followers)
            #user.done = True
            mylogger.log(str(users_done_num) +  " done " +  str(qres.qsize()) + " qres " + str(qreq.qsize()) +  " qreq")
            
            #for fr in set(item[0] + item[1]):
       
            #    x = fr.screen_name
            #    userfq = UserAttr.query.filter_by(uname=x)
            #    user_found = False
            #    if userfq.count() == 1:
            #        userf = userfq.one()
            #        user_found = True
            #    if not user_found or userf.requested == False:
            #    #if x not in users_requested:
            #        if users_requested_num < MAX_NODE_NUM:
            #            if not user_found:
            #                userf = UserAttr(x, attr=fr, requested=False, done=False)
            #            else:
            #                userf.SetAttr(fr)
                 
                        
                        #qreq.put(x)
            #elixir.session.commit()
            if qreq.qsize() < PROCESS_NUM:
                new_reqset = UserAttr.query.filter_by(requested=False).slice(1, 10 * PROCESS_NUM)
                for new_req in new_reqset:
                    new_req.requested = True
                    qreq.put(new_req.uname)
                    mylogger.log("put %s" % new_req.uname)
                    users_requested_num += 1
                elixir.session.commit()

        terminate_children(qreq)

        for p in processes:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        print "interrupt received"


    return res

def worker(qreq, qres, fq, logq, dblock):
    mylogger = myLogging(logq)
    try:
        from model import *
        #renew_elixir_session()
        initiate_elixir()
        mylogger.log("in worker")
        while True:
            uname = qreq.get()
            #mylogger.log("got new req " + uname)
            if uname != "TERMINATE":
                res = get_user_friends_and_followers(uname)
                mylogger.log("got result for " + uname)
                dblock.acquire()
                mylogger.log("lock acquired")
                friends = [x.screen_name for x in res[0]]
                followers = [x.screen_name for x in res[1]]
                #mylogger.log("writing to file queue")
                fq.put(("links", uname + ":" + ",".join(friends) + ":" + ",".join(followers) + "\n"))

                #mylogger.log("written to file queue")
                user = UserAttr.query.filter_by(uname=uname).one()
            
                #users_done.add(res.keys()[0])
                #should set friends and followers here (if needed)
                #user.SetFriends(friends)
                #user.SetFollowers(followers)
                user.done = True
                mylogger.log("user for %s retreived" % uname)
            
            #lf.write(uname + ":" + ",".join(friends) + ":" + ",".join(followers) + "\n")
                for fr in set(res[0] + res[1]):
                    x = fr.screen_name
                    userfq = UserAttr.query.filter_by(uname=x)
                    user_found = False
                    if userfq.count() == 1:
                        userf = userfq.one()
                        user_found = True
                    if not user_found or userf.requested == False:
                    #if x not in users_requested:
                        #af.write(str(fr) + "\n")
                        #users_requested.add(x)
                        if not user_found:
                            #should set user attr here if needed
                            #userf = UserAttr(x, attr=fr, requested=False, done=False)
                            fq.put(("attributes", str(fr) + "\n"))
                            userf = UserAttr(x, attr=twitter.User(), requested=False, done=False)
                        else:
                            #userf.SetAttr(fr)
                            pass
                 
                        
                        #qreq.put(x)
                mylogger.log("committing to db")
                elixir.session.commit()
                dblock.release()
                mylogger.log("lock released")

            else:
                qreq.task_done()
                return
            qres.put({uname:res})
            qreq.task_done()
    except Exception as e:
        mylogger.log("Error " + str(e))

def get_user_friends_and_followers(uname):
    user = "AliZand3"
    passwd = "zandp@ss"
    words = ['test', "security"]
    consumer_key = "vEV8KWRBS0WXbAXonGruzg"
    consumer_secret = "wJySs4mlcGBTqZvQgKN4Y2kHGbj8IzZE6TdLxELbxJ8"
    access_token = "509393232-gpefGK1coYaHssRtPRvBJVtTbaOMA4Vs4OiNYQ6A"
    access_token_secret = "gOlkvAxjwl6KdXmtVpMlLA6H8Q5bdAC0IrGg2oha3U"
    to_wait = cap_wait
    cap_got = False
    cap = 0


    api = twitter.Api(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token_key=access_token, access_token_secret=access_token_secret)

    while not cap_got or cap < 10:
        while not cap_got:
            try:
                cap = api.GetRateLimitStatus()["remaining_hits"]
                cap_got = True
            except Exception as e:
                print e
        if cap < 10:
            cap_got = False
            print uname, "out of cap, waiting for %d sec" % to_wait
            time.sleep(to_wait)
            to_wait *= 1.5
            to_wait = min(to_wait, max_wait)
            if to_wait >= max_wait:
                api.close()
                api = twitter.Api(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token_key=access_token, access_token_secret=access_token_secret)
    try:    
        friends = api.GetFriends(uname)
        followers = api.GetFollowers(uname)
    except Exception as e:
        print e
        return [[],[]]

    return [friends, followers]

def build_user_communities(uname):
    user = "a valide username should go here"
    passwd = "the passwd of the account"
    words = ['test', "security"]
    consumer_key = "consumer key"
    consumer_secret = "consumer secret"
    access_token = "access token"
    access_token_secret = "access token secret" #consumer key, consumer key
    #secret, access token, and access token secret can be acquired from twitter
    #oath2


    api = twitter.Api(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token_key=access_token, access_token_secret=access_token_secret)
    while api.GetRateLimitStatus()["remaining_hits"] < 100:
        print uname, "out of cap"
        time.sleep(10)
        
    friends = list(set(api.GetFriends(uname) + api.GetFollowers(uname)))

    g = igraph.Graph(directed=True)
    nodeno = {}
    last_node = -1
    b = {}

    for x in friends:
        print x.screen_name
        name = x.screen_name
        try:
            while api.GetRateLimitStatus()["remaining_hits"] < 10:
                print uname, "out of cap"
                time.sleep(10)
            b[name] = [y for y in list(set(api.GetFriends(name) + api.GetFollowers(name))) if y in friends]
            if name not in nodeno:
                last_node += 1
                nodeno[name] = last_node
                g.add_vertices(1)
            for y in b[name]:
                yname = y.screen_name
                if yname not in nodeno:
                    last_node += 1
                    nodeno[yname] = last_node
                    g.add_vertices(1)
                if nodeno[yname] not in g.neighbors(nodeno[name]):
                    g.add_edges([(nodeno[name], nodeno[yname])])
        except Exception as e:
            print e
            pass
    
    nodename = {}
    for x in nodeno:
        nodename[nodeno[x]] = x

    #delete unidirectional edges
    for e in g.es:
        (v1,v2) = e.tuple
        if v1 not in g.neighbors(v2):
            g.delete_edges([(v1, v2)])

    cliques = g.maximal_cliques()
    communities = merge_communities(cliques)
    return (b, g, nodename, nodeno, [x for x in communities if len(x) > 2])

def totuple(s):
    return tuple(sorted(list(s)))

def merge_communities(cliques):
    merged = set([])
    g = igraph.Graph(0)
    nodeno = {}
    nonode = {}
    lastnode = -1
    for i in range(len(cliques)):
        c1 = cliques[i]
        cs1 = set(list(c1))
        if totuple(cs1) not in nodeno:
            lastnode += 1
            nodeno[totuple(cs1)] = lastnode
            nonode[lastnode] = cs1
            g.add_vertices(1)
        for j in range(i + 1, len(cliques)):
            c2 = cliques[j]
            cs2 = set(list(c2))
            if totuple(cs2) not in nodeno:
                lastnode += 1
                nodeno[totuple(cs2)] = lastnode
                nonode[lastnode] = cs2
                g.add_vertices(1)
            if len(cs1) == len(cs2):
                if len(cs1 & cs2) == len(cs1) - 1:
                    g.add_edges([(nodeno[totuple(cs1)], nodeno[totuple(cs2)])])

    for comp in g.components():
        community = set([])
        for c in comp:
            community |= nonode[c]
   
        merged.add(totuple(community))

    return list(merged)

    return merged
                    
def main():
    multiprocess_mine(["azand", "bboe"])


if __name__ == "__main__":
    main()
            
    #filterstream = tweetstream.FilterStream(username=user, password = passwd, track=words)
    #for tweet in filterstream:
    #    pass
    #filterstream.close()
