import sys, os, gzip, re, datetime
from uuid import uuid1 as uid 
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
 
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_words(file_line):
    linex = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    return linex.split(file_line.replace("'", "_"))

def create_Table():
    session.execute("""
            CREATE TABLE IF NOT EXISTS nasalogs (
                host TEXT,                
                datetime TIMESTAMP,
                path TEXT,
                bytes INT,
                id UUID,
                PRIMARY KEY (host,id)
            )
            """)
def main(inputs,table):
    create_Table()
    session.execute("""TRUNCATE nasalogs;""")
    insert_log = session.prepare("INSERT INTO "+ table + " (host,datetime,path,bytes,id) VALUES (?,?,?,?,?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    c = 0
    for g_file in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, g_file), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                w = get_words(line)
                if len(w) > 4:
                	c += 1
                	batch.add(insert_log,(w[1],datetime.datetime.strptime(w[2], '%d/%b/%Y:%H:%M:%S'),w[3],int(w[4]),uid()))
                if (c == 400):
                	session.execute(batch)
                	batch.clear()  
                	c = 0
        	  
    session.execute(batch)
    cluster.shutdown()


if __name__ == "__main__":
    inputs = sys.argv[1]    
    key_space = sys.argv[2]
    table = sys.argv[3]
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(key_space)
    main(inputs,table)