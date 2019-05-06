import properties as cfg
import psycopg2

class RedShiftConn(object):

  def __init__(self):
    self.myattr = 0

  def bar(self):
    self.myattr += 1
    return self

  def getConnectionObj():
        con=psycopg2.connect(dbname= cfg.redshift['dbname'], host=cfg.redshift['host'], port= cfg.redshift['port'], user= cfg.redshift['user'], password= cfg.redshift['password'])
        #print("Redshift Connection -> ", con)
        return con