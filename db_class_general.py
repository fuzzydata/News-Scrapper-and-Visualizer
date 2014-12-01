__author__ = 'anrc'

#TODO : ADD META TABLE, GENERALIZE INSERT STATEMENT

import sqlite3,subprocess,shlex,re,datetime

from time import sleep
from sys import exit
class DBManager(object):

    def __init__(self,conn):
        self.conn = conn
        self.cursor = self.conn.cursor()


    def create_table(self,table_name):

        if self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            print 'Table "%s" already exists in database' %table_name
            return False
        else:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS %s \
            (
            Author          TEXT   , \
            Tweet           TEXT        NOT NULL, \
            Tweet_id        INTEGER     UNIQUE NOT NULL, \
            Time            TEXT        NOT NULL,  \
            Favorite_count  INTEGER,            \
            Retweet_count   INTEGER , \
            Hashtags        Text)
            ''' %table_name)
            print 'Table "%s" has been created' %table_name
            return


    def create_table_with_structure(self,new_table,old_table):
        #COMMAND TO COPY CONTENTS BETWEEN TWO SIMILAR TABLES
        #('INSERT INTO Destination_Table (Id,Title,Location,Timestamp,Organization,Link,Entities,Relevance,Sentiment) SELECT Id,Title,Location,Timestamp,Organization,Link,Entities,Relevance,Sentiment FROM source_TABLE')
        self.conn.execute('CREATE TABLE %s AS SELECT * FROM %s WHERE 0'%(new_table,old_table))
        self.conn.commit()
        return

    def get_coloumn_names(self,table_name):
        cursor = self.conn.execute('select * from %s' %table_name)
        return list(map(lambda x: x[0], cursor.description))
        pass

    def insert_record(self,table_name,**args):
        if not self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            print 'Table "%s" doesnt exist' %table_name
            return False
        success_cnt = 0
        try:

            #if tweet.retweeted ==True or 'RT @' in tweet.text:
                #mentions =','.join([z['screen_name'] for z in tweet.entities['user_mentions']])
                #hashtags = ', '.join([item['text'] for item in tweet.entities['hashtags']])

                fields = self.get_coloumn_names(table_name)
                fields_str = '(' +','.join(fields) + ')'
                values_str = '('+','.join(['?']*len(fields)) + ')'

                data  = []
                for item in fields:
                    data.append(args[item])
                data = tuple(data)
                self.cursor.execute('''INSERT INTO %s %s VALUES %s'''%(table_name,fields_str,values_str),data);
                success_cnt += 1

        except sqlite3.OperationalError as e:
            print str(e),e.args
            #print '''%s,%s,%d,%s,%d,%d)'''%(tweet.user.name,tweet.text,tweet.id,tweet.created_at.strftime('%Y-%m-%d : %H:%M:%S'),tweet.favorite_count,tweet.retweet_count)
        if success_cnt:
            subprocess.Popen(['notify-send','-t','1','%d records added till ID = %d' %(success_cnt,self.cursor.lastrowid)])
            self.conn.commit()

        return

    def update_record(self,table_name,rowid,**args):
        if not self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            print 'Table "%s" doesnt exist' %table_name
            return False
        success_cnt = 0
        try:

                fields = self.get_coloumn_names(table_name)
                #fields_str = '(' +','.join(fields) + ')'
                #values_str = '('+','.join(['?']*len(fields)) + ')'
                cmd_list = []
                data  = []
                for item in fields:
                    try:
                        data.append(args[item])

                    except KeyError:
                        continue
                    cmd_list.append(item + '= "' + args[item] + '"')
                cmd = ', '.join(cmd_list)
                #print '''update %s set %s where rowid=%d'''%(table_name,cmd,rowid)
                self.cursor.execute('''update %s set %s where rowid=?'''%(table_name,cmd),(rowid,));
                success_cnt += 1

        except sqlite3.OperationalError as e:
            print str(e),e.args
            return False
            #print '''%s,%s,%d,%s,%d,%d)'''%(tweet.user.name,tweet.text,tweet.id,tweet.created_at.strftime('%Y-%m-%d : %H:%M:%S'),tweet.favorite_count,tweet.retweet_count)
        if success_cnt:
            subprocess.Popen(['notify-send','-t','2','%d record updated at ID = %d' %(success_cnt,rowid)])
            self.conn.commit()

        return




    def get_tables_list(self):
        table_list = []
        print 'Fetching existing tables from sqlite_master...'
        for table in self.conn.execute("select name from sqlite_master where type = 'table'").fetchall():
            table_list.append(table[0])
        try:
            table_list.remove('sqlite_sequence')

        except ValueError as e:
            print "Sqlite_Sequence not in List "
        if table_list:
            return table_list
        else:
            return False

    def clear_table(self,table_name):
        self.conn.execute('delete  from %s'%(table_name))
        print 'Table contents have been deleted'
        self.conn.commit()

    def drop_table(self,table_name):
        if  table_name == 'sqlite_sequence':
            print 'Cannot drop table "sqlite_sequence"'
            return False

        if self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            self.conn.execute('DROP TABLE %s' %(table_name))
            print 'Table "%s" has been deleted' %(table_name)
            return
        else:
            print 'Table "%s" doesnt exist' %(table_name)
            return False

    def other_sqlite_ops(self,cmd):
        return self.cursor.execute(cmd).fetchall()

    def create_table_general(self,cmd):
        self.cursor.execute(cmd)
        self.conn.commit()

#con = sqlite3.connect('/home/anrc/PycharmProjects/Aggregator/news_check.db')
#D = DBManager(con)
#print D.get_coloumn_names('ndtv')
#D.insert_record(table_name='ndtv',URL='a',Title='b',Timestamp='n')#D.clear_table('ndtv')
#print D.get_coloumn_names('ndtv')

