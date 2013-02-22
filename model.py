import twitter
import json
import elixir


class UserAttr(elixir.Entity):
    #idnum = elixir.Field(elixir.Integer, primary_key=True)
    uname = elixir.Field(elixir.String(1000), primary_key=True)
    attr = elixir.Field(elixir.Text(204800))
    requested = elixir.Field(elixir.Boolean, index=True, unique=False)
    done = elixir.Field(elixir.Boolean)
    friends = elixir.Field(elixir.Text(200000000))
    followers = elixir.Field(elixir.Text(200000000))

    def SetFriends(self, friends):
        self.friends = json.dumps(friends)

    def SetFollowers(self, followers):
        self.followers = json.dumps(followers)

    def SetAttr(self, attr):
        self.attr = attr.AsJsonString()
        

    def GetFriends(self):
        return json.loads(self.friends)
    
    def GetFollowers(self):
        return json.loads(self.followers)
    
    def GetAttr(self):
        return twitter.User.NewFromJsonDict(self.attr)

    def Friends(self):
        return json.loads(self.friends)

    def Followers(self):
        return json.loads(self.followers)

    def __init__(self, uname, attr=twitter.User(), requested=False, done = False, friends={}, followers={}):
        #self.idnum = idnum
        self.uname = uname
        self.SetAttr(attr)
        self.requested = requested
        self.done = done
        self.SetFriends(friends)
        self.SetFollowers(followers)
        
    def __repr__(self):
        reqstr = "requested"
        if not self.requested:
            reqstr = "not requested"
        return '<user "%s": %s---%s>' % (uname, reqstr, attr)

